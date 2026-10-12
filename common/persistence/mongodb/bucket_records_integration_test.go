//go:build integration

package mongodb

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	taskspkg "go.temporal.io/server/service/history/tasks"
)

func TestBucketHistoryRecordsSharePlacementAndKeepTaskBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	factory, cfg := newFencingTestFactory(t, "bucket_records")
	requireShardedTopology(t, factory)
	shards, err := factory.NewShardStore()
	require.NoError(t, err)
	_, err = shards.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: 17, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 100, persistence.NewDataBlob([]byte("owner"), enumspb.ENCODING_TYPE_PROTO3.String()), nil
		},
	})
	require.NoError(t, err)
	store, err := factory.NewExecutionStore()
	require.NoError(t, err)
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "split", Value: cfg.DatabaseName + ".history_records"},
		{Key: "middle", Value: bson.D{{Key: "shard_id", Value: int32(17)}, {Key: "storage_bucket", Value: int32(32)}}},
	}))
	for bucket, target := range map[int32]string{12: "shard-rs-0", 53: "shard-rs-1"} {
		require.NoError(t, runAdminCommand(ctx, factory, bson.D{
			{Key: "moveChunk", Value: cfg.DatabaseName + ".history_records"},
			{Key: "find", Value: bson.D{{Key: "shard_id", Value: int32(17)}, {Key: "storage_bucket", Value: bucket}}},
			{Key: "to", Value: target},
		}))
	}
	fireTime := time.Date(2026, 10, 7, 12, 0, 0, 123456789, time.UTC)
	for index, workflow := range []string{"wf", "workflow-0"} {
		taskID := int64(index + 1)
		_, err := store.CreateWorkflowExecution(ctx, &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID: 17, RangeID: 100, Mode: persistence.CreateWorkflowModeBrandNew,
			NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
				NamespaceID: "ns", WorkflowID: workflow, RunID: "run",
				ExecutionInfo: &persistencespb.WorkflowExecutionInfo{},
				ExecutionState: &persistencespb.WorkflowExecutionState{
					RunId: "run", CreateRequestId: workflow,
					State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				},
				NextEventID: 2, DBRecordVersion: 1,
				Tasks: map[taskspkg.Category][]persistence.InternalHistoryTask{
					taskspkg.CategoryTransfer: {{Key: taskspkg.NewImmediateKey(taskID), Blob: persistence.NewDataBlob([]byte("transfer"), enumspb.ENCODING_TYPE_PROTO3.String())}},
					taskspkg.CategoryTimer:    {{Key: taskspkg.NewKey(fireTime.Add(time.Duration(index)), taskID), Blob: persistence.NewDataBlob([]byte("timer"), enumspb.ENCODING_TYPE_PROTO3.String())}},
				},
			},
		})
		require.NoError(t, err)
	}
	pool := factory.database.Collection("history_records")
	require.Equal(t, 2, explainShardCount(ctx, t, factory, "history_records", bson.D{{Key: "shard_id", Value: int32(17)}}))
	for _, bucket := range []int32{53, 12} {
		require.Equal(t, 1, explainShardCount(ctx, t, factory, "history_records", bson.D{{Key: "shard_id", Value: int32(17)}, {Key: "storage_bucket", Value: bucket}}))
		for kind, expected := range map[string]int64{"fence": 1, "state": 1, "current": 1, "task": 2} {
			count, err := pool.CountDocuments(ctx, bson.M{"shard_id": int32(17), "storage_bucket": bucket, "record_kind": kind})
			require.NoError(t, err)
			require.Equal(t, expected, count, "bucket %d record kind %s", bucket, kind)
		}
	}
	for _, category := range []taskspkg.Category{taskspkg.CategoryTransfer, taskspkg.CategoryTimer} {
		request := &persistence.GetHistoryTasksRequest{ShardID: 17, TaskCategory: category, BatchSize: 1}
		page, err := store.GetHistoryTasks(ctx, request)
		require.NoError(t, err)
		require.Len(t, page.Tasks, 1)
		require.Equal(t, int64(1), page.Tasks[0].Key.TaskID)
		require.NotEmpty(t, page.NextPageToken)
		request.NextPageToken = page.NextPageToken
		page, err = store.GetHistoryTasks(ctx, request)
		require.NoError(t, err)
		require.Len(t, page.Tasks, 1)
		require.Equal(t, int64(2), page.Tasks[0].Key.TaskID)
		if category == taskspkg.CategoryTimer {
			require.True(t, fireTime.Add(time.Nanosecond).Equal(page.Tasks[0].Key.FireTime))
		}
		require.NoError(t, store.RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{ShardID: 17, TaskCategory: category}))
	}
	for kind, expected := range map[string]int64{"fence": 64, "state": 2, "current": 2, "task": 0} {
		count, err := pool.CountDocuments(ctx, bson.M{"shard_id": int32(17), "record_kind": kind})
		require.NoError(t, err)
		require.Equal(t, expected, count, "task acknowledgement must preserve %s", kind)
	}
	listing, err := store.ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{ShardID: 17, PageSize: 10})
	require.NoError(t, err)
	require.Len(t, listing.States, 2)
	for _, workflow := range []string{"wf", "workflow-0"} {
		_, err := store.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{ShardID: 17, NamespaceID: "ns", WorkflowID: workflow, RunID: "run"})
		require.NoError(t, err)
		current, err := store.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{ShardID: 17, NamespaceID: "ns", WorkflowID: workflow})
		require.NoError(t, err)
		require.Equal(t, "run", current.RunID)
	}
}

func TestMatchingRecordsKeepFairSubqueuesWithTheirQueueFence(t *testing.T) {
	for _, fair := range []bool{false, true} {
		t.Run(map[bool]string{false: "standard", true: "fair"}[fair], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			t.Cleanup(cancel)
			factory, _ := newFencingTestFactory(t, "matching_records")
			var store persistence.TaskStore
			var err error
			if fair {
				store, err = factory.NewFairTaskStore()
			} else {
				store, err = factory.NewTaskStore()
			}
			require.NoError(t, err)
			info := persistence.NewDataBlob([]byte("queue"), enumspb.ENCODING_TYPE_PROTO3.String())
			require.NoError(t, store.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
				NamespaceID: "ns", TaskQueue: "queue", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 100, TaskQueueInfo: info,
			}))
			subqueue, pass := 0, int64(0)
			if fair {
				subqueue, pass = 3, 1
			}
			_, err = store.CreateTasks(ctx, &persistence.InternalCreateTasksRequest{
				NamespaceID: "ns", TaskQueue: "queue", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, RangeID: 100, TaskQueueInfo: info,
				Tasks: []*persistence.InternalCreateTask{{TaskId: 1, Subqueue: subqueue, TaskPass: pass, Task: info}},
			})
			require.NoError(t, err)
			for kind, expected := range map[string]int64{"queue": 1, "task": 1} {
				count, err := factory.database.Collection("matching_records").CountDocuments(ctx, bson.M{"record_kind": kind})
				require.NoError(t, err)
				require.Equal(t, expected, count)
			}
			tasks, err := store.GetTasks(ctx, &persistence.GetTasksRequest{
				NamespaceID: "ns", TaskQueue: "queue", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Subqueue: subqueue, InclusiveMinPass: pass, ExclusiveMaxTaskID: math.MaxInt64, PageSize: 10,
			})
			require.NoError(t, err)
			require.Len(t, tasks.Tasks, 1)
			maxPass := int64(0)
			if fair {
				maxPass = 2
			}
			deleted, err := store.CompleteTasksLessThan(ctx, &persistence.CompleteTasksLessThanRequest{
				NamespaceID: "ns", TaskQueueName: "queue", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				Subqueue: subqueue, ExclusiveMaxPass: maxPass, ExclusiveMaxTaskID: math.MaxInt64, Limit: 10,
			})
			require.NoError(t, err)
			require.Equal(t, 1, deleted)
			queue, err := store.GetTaskQueue(ctx, &persistence.InternalGetTaskQueueRequest{NamespaceID: "ns", TaskQueue: "queue", TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW})
			require.NoError(t, err)
			require.Equal(t, int64(100), queue.RangeID)
			listing, err := store.ListTaskQueue(ctx, &persistence.ListTaskQueueRequest{PageSize: 10})
			require.NoError(t, err)
			require.Len(t, listing.Items, 1)
		})
	}
}
