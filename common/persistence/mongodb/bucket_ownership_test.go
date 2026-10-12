package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

func TestShardStoreInitializationCreatesEveryBucketFence(t *testing.T) {
	db := newFakeMongoDatabase(t)
	owner := newFakeCollection(t)
	owner.findQueue = append(owner.findQueue, fakeSingleResult{err: mongo.ErrNoDocuments})
	db.collections[collectionShards] = owner
	fences := newFakeCollection(t)
	db.collections["history_records"] = fences
	store, err := NewShardStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), "test", true)
	require.NoError(t, err)
	_, err = store.GetOrCreateShard(t.Context(), &persistence.InternalGetOrCreateShardRequest{
		ShardID: 17,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 100, persistence.NewDataBlob([]byte("owner"), enumspb.ENCODING_TYPE_PROTO3.String()), nil
		},
	})
	require.NoError(t, err)
	require.Len(t, owner.insertDocs, 1)
	require.Len(t, fences.insertManyDocs, 1)
	require.Len(t, fences.insertManyDocs[0], 64, "all fences must exist before shard ownership is published")
}

func TestShardStoreRejectsTransactionsDisabled(t *testing.T) {
	_, err := NewShardStore(newFakeMongoDatabase(t), &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), "test", false)
	require.ErrorContains(t, err, "requires transactions-enabled topology")
}

func TestShardStoreDoesNotRepairMissingBucketFences(t *testing.T) {
	db := newFakeMongoDatabase(t)
	owner := newFakeCollection(t)
	doc := &shardDocument{ShardID: 17, RangeID: 100, Data: []byte("owner"), DataEncoding: "Proto3", StorageBucketCount: 64, StorageModelVersion: mongodbschema.StorageModelVersion}
	owner.enqueueFind(doc)
	owner.enqueueFind(doc)
	db.collections[collectionShards] = owner
	fences := newFakeCollection(t)
	db.collections["history_records"] = fences
	store, err := NewShardStore(db, &fakeMongoClient{}, config.MongoDB{}, log.NewTestLogger(), "test", true)
	require.NoError(t, err)
	_, err = store.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{ShardID: 17})
	require.Error(t, err, "an owner without its complete fence set must not serve writes")
	require.Empty(t, fences.insertManyDocs, "missing fences must not be synthesized")
	require.Empty(t, owner.updateFilters)
}

func TestWorkflowStorageBucketStableIdentity(t *testing.T) {
	for _, test := range []struct {
		workflow string
		bucket   int32
	}{{"wf", 53}, {"workflow-0", 12}, {"workflow-1", 51}, {"workflow-2", 29}} {
		t.Run(test.workflow, func(t *testing.T) {
			require.Equal(t, test.bucket, workflowStorageBucket("ns", test.workflow))
		})
	}
}

func TestBucketOwnershipRequiresIdentity(t *testing.T) {
	store := &executionStore{db: newFakeMongoDatabase(t)}
	for _, identity := range [][2]string{{"", "wf"}, {"ns", ""}} {
		require.ErrorContains(t, store.assertShardRangeID(t.Context(), 17, 100, identity[0], identity[1]), "requires namespace and workflow identity")
	}
}

func TestWorkflowBucketsFenceEveryIdentity(t *testing.T) {
	db := newFakeMongoDatabase(t)
	fences := newFakeCollection(t)
	fences.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	fences.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	db.collections[collectionHistoryRecords] = fences
	store := &executionStore{db: db}
	require.NoError(t, store.assertWorkflowBuckets(t.Context(), 17, 100,
		workflowIdentity{"ns", "wf"}, workflowIdentity{"ns", "workflow-0"}, workflowIdentity{"ns", "wf"},
	))
	require.Equal(t, []interface{}{
		bson.M{"shard_id": int32(17), "storage_bucket": int32(12), "record_kind": "fence", "range_id": int64(100)},
		bson.M{"shard_id": int32(17), "storage_bucket": int32(53), "record_kind": "fence", "range_id": int64(100)},
	}, fences.updateFilters)
}
