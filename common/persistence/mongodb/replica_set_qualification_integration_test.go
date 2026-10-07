//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	replicaSetWorkloadWorkers    = 8
	replicaSetTasksPerWorker     = 100
	replicaSetWorkloadTaskTotal  = replicaSetWorkloadWorkers * replicaSetTasksPerWorker
	replicaSetStepDownRetryAfter = 100 * time.Millisecond
)

func TestCreateTasksSurvivesThreeMemberReplicaSetStepdowns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "replica_set_stepdown")
	requireThreeMemberReplicaSet(ctx, t, factory)

	baseStore, err := factory.NewTaskStore()
	require.NoError(t, err)
	store, ok := baseStore.(*taskStore)
	require.True(t, ok)
	taskQueueInfo := persistence.NewDataBlob([]byte("stepdown-queue"), enumspb.ENCODING_TYPE_PROTO3.String())
	require.NoError(t, store.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "stepdown-queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
	}))

	pause := newFencingPause()
	store.db = &replicaSetStepdownDatabase{
		Database: factory.database,
		pause:    pause,
	}
	transactionDone := make(chan error, 1)
	go func() {
		_, err := store.CreateTasks(ctx, newReplicaSetCreateTasksRequest(taskQueueInfo, 1))
		transactionDone <- err
	}()

	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for transaction to reach the stepdown boundary")
	}
	stepDownPrimary(ctx, t, factory)
	pause.release()
	select {
	case err := <-transactionDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for transaction after primary stepdown")
	}
	requireTransactionTestTaskCount(ctx, t, factory, 1)

	store.db = factory.database
	runReplicaSetWorkload(ctx, t, factory, store, taskQueueInfo)
	requireTransactionTestTaskCount(ctx, t, factory, replicaSetWorkloadTaskTotal+1)
}

func runReplicaSetWorkload(
	ctx context.Context,
	t *testing.T,
	factory *Factory,
	store *taskStore,
	taskQueueInfo *commonpb.DataBlob,
) {
	t.Helper()
	progress := make(chan struct{})
	errorsCh := make(chan error, replicaSetWorkloadWorkers)
	var workers sync.WaitGroup
	for worker := range replicaSetWorkloadWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range replicaSetTasksPerWorker {
				taskID := int64(2 + worker*replicaSetTasksPerWorker + task)
				err := createReplicaSetTaskWithRetry(ctx, factory, store, taskQueueInfo, taskID)
				if err != nil {
					var commandError mongo.CommandError
					errors.As(err, &commandError)
					errorsCh <- fmt.Errorf(
						"task %d (%T, code %d, labels %v, transient %t, unknown commit %t): %w",
						taskID,
						err,
						commandError.Code,
						commandError.Labels,
						mongoErrorHasLabel(err, "TransientTransactionError"),
						mongoErrorHasLabel(err, "UnknownTransactionCommitResult"),
						err,
					)
					return
				}
				progress <- struct{}{}
			}
		}()
	}

	completed := 0
	for _, threshold := range []int{50, 250, 500} {
		completed = waitForWorkloadProgress(ctx, t, progress, errorsCh, completed, threshold)
		stepDownPrimary(ctx, t, factory)
	}
	waitForWorkloadProgress(ctx, t, progress, errorsCh, completed, replicaSetWorkloadTaskTotal)
	workers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
}

func createReplicaSetTaskWithRetry(
	ctx context.Context,
	factory *Factory,
	store *taskStore,
	taskQueueInfo *commonpb.DataBlob,
	taskID int64,
) error {
	for {
		_, err := store.CreateTasks(ctx, newReplicaSetCreateTasksRequest(taskQueueInfo, taskID))
		if err == nil {
			return nil
		}

		persisted, findErr := replicaSetTaskExists(ctx, factory, taskID)
		if findErr == nil && persisted {
			return nil
		}
		if findErr != nil && !errors.Is(findErr, mongo.ErrNoDocuments) {
			return fmt.Errorf("verify task %d after write error: %w", taskID, findErr)
		}

		var unavailable *serviceerror.Unavailable
		if !errors.As(err, &unavailable) &&
			!mongoErrorHasLabel(err, "RetryableWriteError") &&
			!mongoErrorHasLabel(err, "TransientTransactionError") &&
			!mongoErrorHasLabel(err, "UnknownTransactionCommitResult") {
			return err
		}
		if ctx.Err() != nil {
			return errors.Join(err, ctx.Err())
		}
	}
}

func replicaSetTaskExists(ctx context.Context, factory *Factory, taskID int64) (bool, error) {
	var task taskDocument
	err := factory.database.Collection(collectionTasks).FindOne(ctx, bson.M{
		"_id": taskDocumentID(
			"namespace",
			"stepdown-queue",
			enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			persistence.SubqueueZero,
			0,
			taskID,
		),
	}).Decode(&task)
	if err != nil {
		return false, err
	}
	return task.TaskID == taskID && string(task.Task) == fmt.Sprintf("task-%d", taskID), nil
}

func mongoErrorHasLabel(err error, label string) bool {
	for err != nil {
		if labeled, ok := err.(mongo.LabeledError); ok && labeled.HasErrorLabel(label) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func newReplicaSetCreateTasksRequest(
	taskQueueInfo *commonpb.DataBlob,
	taskID int64,
) *persistence.InternalCreateTasksRequest {
	return &persistence.InternalCreateTasksRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "stepdown-queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
		Tasks: []*persistence.InternalCreateTask{{
			TaskId:   taskID,
			Task:     persistence.NewDataBlob([]byte(fmt.Sprintf("task-%d", taskID)), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		}},
	}
}

func waitForWorkloadProgress(
	ctx context.Context,
	t *testing.T,
	progress <-chan struct{},
	errorsCh <-chan error,
	completed int,
	target int,
) int {
	t.Helper()
	for completed < target {
		select {
		case <-progress:
			completed++
		case err := <-errorsCh:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatalf("workload reached %d of %d operations before timeout", completed, target)
		}
	}
	return completed
}

func requireThreeMemberReplicaSet(ctx context.Context, t *testing.T, factory *Factory) {
	t.Helper()
	if factory.topologyInfo.Type != client.TopologyReplicaSet {
		t.Skipf("three-member replica set required; connected topology is %s", factory.topologyInfo.Type.String())
	}
	status, err := readReplicaSetStatus(ctx, factory)
	require.NoError(t, err)
	if len(status.Members) != 3 {
		t.Skipf("three-member replica set required; connected topology has %d member(s)", len(status.Members))
	}
}

func stepDownPrimary(ctx context.Context, t *testing.T, factory *Factory) {
	t.Helper()
	before, err := readReplicaSetStatus(ctx, factory)
	require.NoError(t, err)
	oldPrimary := before.primary()
	require.NotEmpty(t, oldPrimary)

	_ = factory.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetStepDown", Value: 30},
		{Key: "force", Value: true},
	}).Err()

	require.Eventually(t, func() bool {
		status, err := readReplicaSetStatus(ctx, factory)
		return err == nil && status.primary() != "" && status.primary() != oldPrimary
	}, 20*time.Second, replicaSetStepDownRetryAfter, "replica set did not elect a different primary after stepping down %s", oldPrimary)
}

type replicaSetStatus struct {
	Members []struct {
		Name     string `bson:"name"`
		StateStr string `bson:"stateStr"`
	} `bson:"members"`
}

func (s replicaSetStatus) primary() string {
	for _, member := range s.Members {
		if member.StateStr == "PRIMARY" {
			return member.Name
		}
	}
	return ""
}

func readReplicaSetStatus(ctx context.Context, factory *Factory) (replicaSetStatus, error) {
	var status replicaSetStatus
	err := factory.client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status)
	return status, err
}

type replicaSetStepdownDatabase struct {
	client.Database
	pause *fencingPause
}

func (d *replicaSetStepdownDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name != collectionTaskQueues {
		return collection
	}
	return &replicaSetStepdownCollection{Collection: collection, pause: d.pause}
}

type replicaSetStepdownCollection struct {
	client.Collection
	pause *fencingPause
}

func (c *replicaSetStepdownCollection) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	if err := c.pause.wait(ctx); err != nil {
		return nil, err
	}
	return c.Collection.UpdateOne(ctx, filter, update, opts...)
}
