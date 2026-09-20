package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	taskspkg "go.temporal.io/server/service/history/tasks"
)

func TestHistoryUniqueIndexesBeginWithShardKey(t *testing.T) {
	executionIndexes := &recordingIndexView{}
	currentIndexes := &recordingIndexView{}
	executions := newFakeCollection(t)
	executions.indexView = executionIndexes
	currentExecutions := newFakeCollection(t)
	currentExecutions.indexView = currentIndexes
	store := &executionStore{executionsCol: executions, currentExecsCol: currentExecutions}

	require.NoError(t, store.ensureExecutionIndexes(context.Background()))
	require.NoError(t, store.ensureCurrentExecutionIndexes(context.Background()))
	require.Len(t, executionIndexes.models, 1)
	require.Equal(t, "shard_id", executionIndexes.models[0].Keys.(bson.D)[0].Key)
	require.True(t, *executionIndexes.models[0].Options.Unique)
	require.Equal(t, "shard_id", currentIndexes.models[0].Keys.(bson.D)[0].Key)
	require.True(t, *currentIndexes.models[0].Options.Unique)
}

func TestHistoryPointDeletesUseShardKey(t *testing.T) {
	const shardID = int32(17)
	executions := newFakeCollection(t)
	currentExecutions := newFakeCollection(t)
	transferTasks := newFakeCollection(t)
	historyNodes := newFakeCollection(t)
	historyBranches := newFakeCollection(t)
	store := &executionStore{
		executionsCol:      executions,
		currentExecsCol:    currentExecutions,
		transferTasksCol:   transferTasks,
		historyNodesCol:    historyNodes,
		historyBranchesCol: historyBranches,
	}

	require.NoError(t, store.DeleteWorkflowExecution(context.Background(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID: shardID, NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run",
	}))
	require.NoError(t, store.DeleteCurrentWorkflowExecution(context.Background(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID: shardID, NamespaceID: "namespace", WorkflowID: "workflow", RunID: "run",
	}))
	require.NoError(t, store.CompleteHistoryTask(context.Background(), &persistence.CompleteHistoryTaskRequest{
		ShardID: shardID, TaskCategory: taskspkg.CategoryTransfer, TaskKey: taskspkg.NewImmediateKey(42),
	}))
	branch := &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "branch"}
	require.NoError(t, store.DeleteHistoryNodes(context.Background(), &persistence.InternalDeleteHistoryNodesRequest{
		ShardID: shardID, BranchInfo: branch, NodeID: 2, TransactionID: 3,
	}))
	require.NoError(t, store.DeleteHistoryBranch(context.Background(), &persistence.InternalDeleteHistoryBranchRequest{
		ShardID: shardID, BranchInfo: branch,
	}))

	for _, rawFilter := range []interface{}{
		executions.deleteFilters[0],
		currentExecutions.deleteFilters[0],
		transferTasks.deleteFilters[0],
		historyNodes.deleteFilters[0],
		historyBranches.deleteFilters[0],
	} {
		require.Equal(t, shardID, rawFilter.(bson.M)["shard_id"])
	}
}

func TestMatchingQueueOperationsUseCompleteShardKey(t *testing.T) {
	const (
		namespace = "namespace"
		queue     = "queue"
	)
	db := newFakeMongoDatabase(t)
	queueCollection := newFakeCollection(t)
	queueCollection.enqueueFind(&taskQueueDocument{RangeID: 11})
	queueCollection.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	db.collections[collectionTaskQueues] = queueCollection
	store := &taskStore{db: db}

	_, err := store.GetTaskQueue(context.Background(), &persistence.InternalGetTaskQueueRequest{
		NamespaceID: namespace,
		TaskQueue:   queue,
		TaskType:    enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	require.NoError(t, err)
	_, err = store.UpdateTaskQueue(context.Background(), &persistence.InternalUpdateTaskQueueRequest{
		NamespaceID:   namespace,
		TaskQueue:     queue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		PrevRangeID:   11,
		RangeID:       12,
		TaskQueueInfo: persistence.NewDataBlob([]byte("metadata"), enumspb.ENCODING_TYPE_PROTO3.String()),
	})
	require.NoError(t, err)

	wantRoute := matchingRoutingFilter(namespace, queue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, persistence.SubqueueZero)
	for _, rawFilter := range []interface{}{queueCollection.findFilters[0], queueCollection.updateFilters[0]} {
		filter := rawFilter.(bson.M)
		for key, value := range wantRoute {
			require.Equal(t, value, filter[key], "missing or incorrect shard-key field %s", key)
		}
	}
}

func TestCreateTasksRoutesFenceAndDocumentsByMatchingShardKey(t *testing.T) {
	const (
		namespace = "namespace"
		queue     = "queue"
	)
	db := newFakeMongoDatabase(t)
	queueCollection := newFakeCollection(t)
	queueCollection.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	tasksCollection := newFakeCollection(t)
	db.collections[collectionTaskQueues] = queueCollection
	db.collections[collectionTasks] = tasksCollection
	store := &taskStore{db: db, mongoClient: &fakeMongoClient{}}

	_, err := store.CreateTasks(context.Background(), &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespace,
		TaskQueue:     queue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       11,
		TaskQueueInfo: persistence.NewDataBlob([]byte("metadata"), enumspb.ENCODING_TYPE_PROTO3.String()),
		Tasks: []*persistence.InternalCreateTask{{
			TaskId:   42,
			Subqueue: 3,
			Task:     persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()),
		}},
	})
	require.NoError(t, err)

	queueFilter := queueCollection.updateFilters[0].(bson.M)
	wantQueueRoute := matchingRoutingFilter(namespace, queue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, persistence.SubqueueZero)
	for key, value := range wantQueueRoute {
		require.Equal(t, value, queueFilter[key], "queue fence must include %s", key)
	}

	require.Len(t, tasksCollection.insertManyDocs, 1)
	require.Len(t, tasksCollection.insertManyDocs[0], 1)
	doc := tasksCollection.insertManyDocs[0][0].(taskDocument)
	require.Equal(t, namespace, doc.NamespaceID)
	require.Equal(t, queue, doc.TaskQueue)
	require.Equal(t, int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW), doc.TaskType)
	require.Equal(t, 3, doc.Subqueue)
}

func TestCompleteTasksDeletionRetainsMatchingShardKey(t *testing.T) {
	db := newFakeMongoDatabase(t)
	tasksCollection := newFakeCollection(t)
	tasksCollection.enqueueFindMany(&struct {
		ID string `bson:"_id"`
	}{ID: "task-id"})
	db.collections[collectionTasks] = tasksCollection
	store := &taskStore{db: db}

	_, err := store.CompleteTasksLessThan(context.Background(), &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        "namespace",
		TaskQueueName:      "queue",
		TaskType:           enumspb.TASK_QUEUE_TYPE_ACTIVITY,
		Subqueue:           4,
		ExclusiveMaxTaskID: 100,
		Limit:              10,
	})
	require.NoError(t, err)
	require.Len(t, tasksCollection.deleteManyFilters, 1)
	filter := tasksCollection.deleteManyFilters[0].(bson.M)
	wantRoute := matchingRoutingFilter("namespace", "queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY, 4)
	for key, value := range wantRoute {
		require.Equal(t, value, filter[key], "delete must include %s", key)
	}
	require.Contains(t, filter, "_id")
}

type recordingIndexView struct {
	models []mongo.IndexModel
}

func (v *recordingIndexView) List(context.Context, ...*options.ListIndexesOptions) (client.Cursor, error) {
	return &noopCursor{}, nil
}

func (v *recordingIndexView) CreateOne(_ context.Context, model mongo.IndexModel, _ ...*options.CreateIndexesOptions) (string, error) {
	v.models = append(v.models, model)
	return "", nil
}

func (v *recordingIndexView) DropOne(context.Context, string, ...*options.DropIndexesOptions) error {
	return nil
}
