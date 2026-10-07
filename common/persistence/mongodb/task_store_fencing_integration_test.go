//go:build integration

package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

func TestTaskStoreRejectsStaleTaskQueueOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, cfg := newFencingTestFactory(t, "task_queue")
	newOwnerStore, err := NewTaskStore(
		factory.database,
		factory.client,
		cfg,
		factory.logger,
		factory.transactionsEnabled,
	)
	require.NoError(t, err)

	oldOwnerStoreValue, err := factory.NewTaskStore()
	require.NoError(t, err)
	oldOwnerStore, ok := oldOwnerStoreValue.(*taskStore)
	require.True(t, ok)

	const (
		namespaceID = "namespace"
		taskQueue   = "task-queue"
		oldRangeID  = int64(50)
		newRangeID  = int64(51)
	)
	oldQueueInfo := persistence.NewDataBlob([]byte("old-owner"), enumspb.ENCODING_TYPE_PROTO3.String())
	newQueueInfo := persistence.NewDataBlob([]byte("new-owner"), enumspb.ENCODING_TYPE_PROTO3.String())
	err = newOwnerStore.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   namespaceID,
		TaskQueue:     taskQueue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       oldRangeID,
		TaskQueueInfo: oldQueueInfo,
	})
	require.NoError(t, err)

	pause := newFencingPause()
	t.Cleanup(pause.release)
	oldOwnerStore.db = &taskQueueFenceBlockingDatabase{
		Database: factory.database,
		pause:    pause,
	}

	staleWriteDone := make(chan error, 1)
	go func() {
		_, err := oldOwnerStore.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
			NamespaceID:   namespaceID,
			TaskQueue:     taskQueue,
			TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			RangeID:       oldRangeID,
			TaskQueueInfo: oldQueueInfo,
			Tasks: []*persistence.InternalCreateTask{
				{
					TaskId:   1,
					Task:     persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()),
					Subqueue: persistence.SubqueueZero,
				},
			},
		})
		staleWriteDone <- err
	}()

	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the old owner to reach the task queue fence")
	}

	_, err = newOwnerStore.UpdateTaskQueue(ctx, &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID:   namespaceID,
		TaskQueue:     taskQueue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       newRangeID,
		PrevRangeID:   oldRangeID,
		TaskQueueInfo: newQueueInfo,
	})
	require.NoError(t, err)
	pause.release()

	select {
	case err = <-staleWriteDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the stale task write")
	}
	var conditionFailed *persistence.ConditionFailedError
	require.ErrorAs(t, err, &conditionFailed)

	count, err := factory.database.Collection(collectionTasks).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Zero(t, count, "stale owner inserted a task")

	queue, err := newOwnerStore.GetTaskQueue(ctx, &persistence.InternalGetTaskQueueRequest{
		NamespaceID: namespaceID,
		TaskQueue:   taskQueue,
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	require.NoError(t, err)
	require.Equal(t, newRangeID, queue.RangeID)
	require.Equal(t, newQueueInfo.Data, queue.TaskQueueInfo.Data)
	require.Equal(t, newQueueInfo.EncodingType, queue.TaskQueueInfo.EncodingType)
}

type taskQueueFenceBlockingDatabase struct {
	client.Database
	pause *fencingPause
}

func (d *taskQueueFenceBlockingDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name != collectionTaskQueues {
		return collection
	}
	return &taskQueueFenceBlockingCollection{
		Collection: collection,
		pause:      d.pause,
	}
}

type taskQueueFenceBlockingCollection struct {
	client.Collection
	pause *fencingPause
}

func (c *taskQueueFenceBlockingCollection) UpdateOne(
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
