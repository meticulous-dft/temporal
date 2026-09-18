//go:build integration

package mongodb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

func TestCreateTasksRetriesTransientTransactionError(t *testing.T) {
	ctx, factory, store, taskQueueInfo := newTaskTransactionTest(t, "transient_error")
	database := &transactionRetryDatabase{
		Database:         factory.database,
		targetCollection: collectionTaskQueues,
		failFirst:        true,
	}
	store.db = database

	_, err := store.CreateTasks(ctx, newTransactionTestCreateTasksRequest(taskQueueInfo))
	require.NoError(t, err)
	require.GreaterOrEqual(t, database.updateAttempts.Load(), int32(2))
	requireTransactionTestTaskCount(ctx, t, factory, 1)
}

func TestCreateTasksRetriesUnknownCommitResult(t *testing.T) {
	ctx, factory, store, taskQueueInfo := newTaskTransactionTest(t, "unknown_commit")
	database := &transactionRetryDatabase{
		Database:         factory.database,
		targetCollection: collectionTaskQueues,
	}
	store.db = database

	enableUnknownCommitResultOnce(ctx, t, factory)
	_, err := store.CreateTasks(ctx, newTransactionTestCreateTasksRequest(taskQueueInfo))
	require.NoError(t, err)
	require.Equal(t, int32(1), database.updateAttempts.Load(), "transaction callback was replayed")
	requireTransactionTestTaskCount(ctx, t, factory, 1)
}

func TestQueueStoreRetriesTransientTransactionError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, cfg := newFencingTestFactory(t, "queue_transient_error")
	baseStore, err := NewQueueStore(
		factory.database,
		factory.client,
		cfg,
		factory.logger,
		factory.metricsHandler,
		factory.transactionsEnabled,
		persistence.NamespaceReplicationQueueType,
	)
	require.NoError(t, err)
	require.NoError(t, baseStore.Init(ctx, persistence.NewDataBlob([]byte("metadata"), enumspb.ENCODING_TYPE_PROTO3.String())))

	database := &transactionRetryDatabase{
		Database:         factory.database,
		targetCollection: collectionQueueMetadata,
		failFirst:        true,
	}
	store, err := NewQueueStore(
		database,
		factory.client,
		cfg,
		factory.logger,
		factory.metricsHandler,
		factory.transactionsEnabled,
		persistence.NamespaceReplicationQueueType,
	)
	require.NoError(t, err)

	err = store.EnqueueMessage(ctx, persistence.NewDataBlob([]byte("message"), enumspb.ENCODING_TYPE_PROTO3.String()))
	require.NoError(t, err)
	require.Equal(t, int32(2), database.updateAttempts.Load())

	count, err := factory.database.Collection(collectionQueueMessages).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestCreateTasksTimeoutAbortsTransactionAndRecovers(t *testing.T) {
	ctx, factory, store, taskQueueInfo := newTaskTransactionTest(t, "transaction_timeout")
	database := &transactionRetryDatabase{
		Database:              factory.database,
		targetCollection:      collectionTaskQueues,
		blockUntilContextDone: true,
	}
	store.db = database

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := store.CreateTasks(timeoutCtx, newTransactionTestCreateTasksRequest(taskQueueInfo))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	requireTransactionTestTaskCount(ctx, t, factory, 0)

	store.db = factory.database
	_, err = store.CreateTasks(ctx, newTransactionTestCreateTasksRequest(taskQueueInfo))
	require.NoError(t, err)
	requireTransactionTestTaskCount(ctx, t, factory, 1)
}

func newTaskTransactionTest(
	t *testing.T,
	name string,
) (context.Context, *Factory, *taskStore, *commonpb.DataBlob) {
	t.Helper()
	factory, _ := newFencingTestFactory(t, name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	storeValue, err := factory.NewTaskStore()
	require.NoError(t, err)
	store, ok := storeValue.(*taskStore)
	require.True(t, ok)

	taskQueueInfo := persistence.NewDataBlob([]byte("task-queue"), enumspb.ENCODING_TYPE_PROTO3.String())
	err = store.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "task-queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
	})
	require.NoError(t, err)

	return ctx, factory, store, taskQueueInfo
}

func newTransactionTestCreateTasksRequest(taskQueueInfo *commonpb.DataBlob) *persistence.InternalCreateTasksRequest {
	return &persistence.InternalCreateTasksRequest{
		NamespaceID:   "namespace",
		TaskQueue:     "task-queue",
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
		Tasks: []*persistence.InternalCreateTask{
			{
				TaskId:   1,
				Task:     persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()),
				Subqueue: persistence.SubqueueZero,
			},
		},
	}
}

func enableUnknownCommitResultOnce(ctx context.Context, t *testing.T, factory *Factory) {
	t.Helper()
	configureFailpointOnAllSeeds(ctx, t, factory.cfg, bson.D{
		{Key: "configureFailPoint", Value: "failCommand"},
		{Key: "mode", Value: bson.D{{Key: "times", Value: 1}}},
		{Key: "data", Value: bson.D{
			{Key: "failCommands", Value: bson.A{"commitTransaction"}},
			{Key: "writeConcernError", Value: bson.D{
				{Key: "code", Value: 91},
				{Key: "errmsg", Value: "injected ambiguous commit result"},
			}},
		}},
	})
}

func configureFailpointOnAllSeeds(
	ctx context.Context,
	t *testing.T,
	cfg config.MongoDB,
	command bson.D,
) func() {
	t.Helper()
	directClients := make([]*mongo.Client, 0, len(cfg.Hosts))
	for _, host := range cfg.Hosts {
		directCfg := cfg
		directCfg.Hosts = []string{host}
		directCfg.ReplicaSet = ""
		clientOptions, _, err := buildMongoOptions(directCfg)
		require.NoError(t, err)
		clientOptions.SetDirect(true)
		directClient, err := mongo.Connect(ctx, clientOptions)
		require.NoError(t, err)
		require.NoError(t, directClient.Ping(ctx, nil))
		require.NoError(t, directClient.Database("admin").RunCommand(ctx, command).Err())
		directClients = append(directClients, directClient)
	}

	var disableOnce sync.Once
	disable := func() {
		disableOnce.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for _, directClient := range directClients {
				if err := directClient.Database("admin").RunCommand(cleanupCtx, bson.D{
					{Key: "configureFailPoint", Value: "failCommand"},
					{Key: "mode", Value: "off"},
				}).Err(); err != nil {
					t.Errorf("failed to disable command failpoint: %v", err)
				}
				if err := directClient.Disconnect(cleanupCtx); err != nil {
					t.Errorf("failed to disconnect failpoint client: %v", err)
				}
			}
		})
	}
	t.Cleanup(disable)
	return disable
}

func requireTransactionTestTaskCount(ctx context.Context, t *testing.T, factory *Factory, expected int64) {
	t.Helper()
	count, err := factory.database.Collection(collectionTasks).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, expected, count)
}

type transactionRetryDatabase struct {
	client.Database
	targetCollection      string
	updateAttempts        atomic.Int32
	failFirst             bool
	blockUntilContextDone bool
}

func (d *transactionRetryDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name != d.targetCollection {
		return collection
	}
	return &transactionRetryCollection{
		Collection: collection,
		database:   d,
	}
}

type transactionRetryCollection struct {
	client.Collection
	database *transactionRetryDatabase
}

func (c *transactionRetryCollection) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	if c.database.blockUntilContextDone {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	attempt := c.database.updateAttempts.Add(1)
	if c.database.failFirst && attempt == 1 {
		return nil, mongo.CommandError{
			Code:    112,
			Message: "injected write conflict",
			Labels:  []string{"TransientTransactionError"},
		}
	}
	return c.Collection.UpdateOne(ctx, filter, update, opts...)
}
