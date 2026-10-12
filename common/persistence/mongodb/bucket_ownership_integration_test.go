//go:build integration

package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	taskspkg "go.temporal.io/server/service/history/tasks"
)

func TestBucketOwnershipRealStores(t *testing.T) {
	for _, scenario := range []string{"complete_initialization", "missing_fence", "aborted_takeover", "owner_publication_failure", "independent_writers"} {
		t.Run(scenario, func(t *testing.T) {
			factory, _ := newFencingTestFactory(t, "bucket_owner")
			requireShardedTopology(t, factory)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			t.Cleanup(cancel)
			shards, err := factory.NewShardStore()
			require.NoError(t, err)
			ownerStore := shards.(*shardStore)
			info := persistence.NewDataBlob([]byte("owner"), enumspb.ENCODING_TYPE_PROTO3.String())
			_, err = shards.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
				ShardID:         17,
				CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 100, info, nil },
			})
			require.NoError(t, err)
			executions, err := factory.NewExecutionStore()
			require.NoError(t, err)
			update := &persistence.InternalUpdateShardRequest{ShardID: 17, PreviousRangeID: 100, RangeID: 101, ShardInfo: info}
			fences := factory.database.Collection(collectionHistoryRecords)
			switch scenario {
			case "complete_initialization":
				_, err := shards.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: 17})
				require.NoError(t, err)
				requireBucketOwnership(t, ctx, factory.database, 100, 64)
				require.NoError(t, shards.UpdateShard(ctx, update))
				requireBucketOwnership(t, ctx, factory.database, 101, 64)
			case "missing_fence":
				result, err := fences.DeleteOne(ctx, bucketFenceFilter(17, 12))
				require.NoError(t, err)
				require.Equal(t, int64(1), result.DeletedCount)
				_, err = shards.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: 17})
				require.Error(t, err)
				var lost *persistence.ShardOwnershipLostError
				require.ErrorAs(t, shards.UpdateShard(ctx, update), &lost)
				requireBucketOwnership(t, ctx, factory.database, 100, 63)
			case "aborted_takeover":
				injected := errors.New("abort after updating every fence")
				ownerStore.db = &bucketOwnerFaultDatabase{Database: factory.database, collection: collectionHistoryRecords, failure: injected}
				require.ErrorIs(t, shards.UpdateShard(ctx, update), injected)
				requireBucketOwnership(t, ctx, factory.database, 100, 64)
				var lost *persistence.ShardOwnershipLostError
				require.ErrorAs(t, executions.AddHistoryTasks(ctx, bucketTaskRequest("wf", 101, 1)), &lost)
				require.NoError(t, executions.AddHistoryTasks(ctx, bucketTaskRequest("wf", 100, 1)))
			case "owner_publication_failure":
				injected := errors.New("abort after owner publication write")
				ownerStore.db = &bucketOwnerFaultDatabase{Database: factory.database, collection: collectionShards, failure: injected}
				require.ErrorIs(t, shards.UpdateShard(ctx, update), injected)
				requireBucketOwnership(t, ctx, factory.database, 100, 64)
			case "independent_writers":
				pause := newFencingPause()
				t.Cleanup(pause.release)
				held, err := factory.NewExecutionStore()
				require.NoError(t, err)
				// Use another store handle so only the held writer encounters the barrier.
				heldStore := *held.(*executionStore)
				heldStore.db = &bucketOwnerFaultDatabase{Database: factory.database, collection: collectionHistoryRecords, pause: pause}
				done := make(chan error, 1)
				go func() { done <- heldStore.AddHistoryTasks(ctx, bucketTaskRequest("workflow-0", 100, 1)) }()
				waitModelPause(ctx, t, pause)
				progressCtx, progressCancel := context.WithTimeout(ctx, 10*time.Second)
				err = executions.AddHistoryTasks(progressCtx, bucketTaskRequest("wf", 100, 2))
				progressCancel()
				pause.release()
				require.NoError(t, waitModelResult(ctx, t, done))
				require.NoError(t, err, "another workflow bucket must commit while the first bucket is locked")
				count, err := factory.database.Collection(collectionHistoryRecords).CountDocuments(ctx, bson.M{"shard_id": int32(17), "record_kind": "task"})
				require.NoError(t, err)
				require.Equal(t, int64(2), count)
			}
		})
	}
}

func bucketTaskRequest(workflow string, rangeID, taskID int64) *persistence.InternalAddHistoryTasksRequest {
	return &persistence.InternalAddHistoryTasksRequest{
		ShardID: 17, RangeID: rangeID, NamespaceID: "ns", WorkflowID: workflow,
		Tasks: map[taskspkg.Category][]persistence.InternalHistoryTask{
			taskspkg.CategoryTransfer: {{Key: taskspkg.NewImmediateKey(taskID), Blob: persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String())}},
		},
	}
}

func requireBucketOwnership(t *testing.T, ctx context.Context, db client.Database, rangeID int64, count int64) {
	t.Helper()
	var owner shardDocument
	require.NoError(t, db.Collection(collectionShards).FindOne(ctx, bson.M{"_id": int32(17)}).Decode(&owner))
	require.Equal(t, rangeID, owner.RangeID)
	matching, err := db.Collection(collectionHistoryRecords).CountDocuments(ctx, bson.M{"shard_id": int32(17), "record_kind": "fence", "range_id": rangeID})
	require.NoError(t, err)
	require.Equal(t, count, matching)
	total, err := db.Collection(collectionHistoryRecords).CountDocuments(ctx, bson.M{"shard_id": int32(17), "record_kind": "fence"})
	require.NoError(t, err)
	require.Equal(t, count, total)
}

type bucketOwnerFaultDatabase struct {
	client.Database
	collection string
	failure    error
	pause      *fencingPause
}

func (d *bucketOwnerFaultDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name == d.collection {
		return &bucketOwnerFaultCollection{Collection: collection, failure: d.failure, pause: d.pause}
	}
	return collection
}

type bucketOwnerFaultCollection struct {
	client.Collection
	failure error
	pause   *fencingPause
}

func (c *bucketOwnerFaultCollection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	result, err := c.Collection.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return nil, err
	}
	return result, c.failure
}

func (c *bucketOwnerFaultCollection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	result, err := c.Collection.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return nil, err
	}
	if c.pause != nil {
		if err := c.pause.wait(ctx); err != nil {
			return nil, err
		}
	}
	return result, c.failure
}
