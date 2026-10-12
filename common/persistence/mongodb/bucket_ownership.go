package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

const collectionHistoryRecords = "history_records"

type bucketFenceDocument struct {
	ID            string `bson:"_id"`
	ShardID       int32  `bson:"shard_id"`
	StorageBucket int32  `bson:"storage_bucket"`
	RecordKind    string `bson:"record_kind"`
	RangeID       int64  `bson:"range_id"`
}

type workflowIdentity struct {
	namespaceID string
	workflowID  string
}

func (s *executionStore) assertWorkflowBuckets(ctx context.Context, shardID int32, rangeID int64, identities ...workflowIdentity) error {
	buckets := make(map[int32]struct{}, len(identities))
	for _, identity := range identities {
		if identity.namespaceID == "" || identity.workflowID == "" {
			return serviceerror.NewInvalidArgument("bucket ownership requires namespace and workflow identity")
		}
		buckets[workflowStorageBucket(identity.namespaceID, identity.workflowID)] = struct{}{}
	}
	ordered := make([]int32, 0, len(buckets))
	for bucket := range buckets {
		ordered = append(ordered, bucket)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, bucket := range ordered {
		if err := assertBucketRangeID(ctx, s.db, shardID, bucket, rangeID); err != nil {
			return err
		}
	}
	return nil
}

// Length framing keeps arbitrary workflow IDs unambiguous. Run identity is deliberately excluded.
func workflowStorageBucket(namespaceID, workflowID string) int32 {
	identity := make([]byte, 8+len(namespaceID)+len(workflowID))
	binary.BigEndian.PutUint32(identity, uint32(len(namespaceID)))
	copy(identity[4:], namespaceID)
	offset := 4 + len(namespaceID)
	binary.BigEndian.PutUint32(identity[offset:], uint32(len(workflowID)))
	copy(identity[offset+4:], workflowID)
	digest := sha256.Sum256(identity)
	return int32(binary.BigEndian.Uint64(digest[:8]) % uint64(mongodbschema.StorageBucketCount))
}

func bucketFenceFilter(shardID, bucket int32) bson.M {
	return bson.M{"shard_id": shardID, "storage_bucket": bucket, "record_kind": "fence"}
}

func workflowRecordFilter(shardID int32, namespaceID, workflowID, kind, id string) bson.M {
	return bson.M{
		"shard_id": shardID, "storage_bucket": workflowStorageBucket(namespaceID, workflowID),
		"record_kind": kind, "_id": id,
	}
}

func bucketFencesFilter(shardID int32, rangeID int64) bson.M {
	return bson.M{
		"shard_id": shardID, "record_kind": "fence", "range_id": rangeID,
		"storage_bucket": bson.M{"$gte": int32(0), "$lt": int32(mongodbschema.StorageBucketCount)},
	}
}

func initialBucketFences(shardID int32, rangeID int64) []interface{} {
	fences := make([]interface{}, mongodbschema.StorageBucketCount)
	for bucket := range fences {
		fences[bucket] = bucketFenceDocument{
			ID: fmt.Sprintf("fence|%d|%d", shardID, bucket), ShardID: shardID,
			StorageBucket: int32(bucket), RecordKind: "fence", RangeID: rangeID,
		}
	}
	return fences
}

func validateBucketFences(ctx context.Context, db client.Database, owner shardDocument) error {
	if owner.StorageBucketCount != mongodbschema.StorageBucketCount || owner.StorageModelVersion != mongodbschema.StorageModelVersion {
		return serviceerror.NewUnavailablef("shard %d has incompatible bucket ownership metadata", owner.ShardID)
	}
	cursor, err := db.Collection(collectionHistoryRecords).Find(ctx,
		bson.M{"shard_id": owner.ShardID, "record_kind": "fence"},
		options.Find().SetSort(bson.D{{Key: "storage_bucket", Value: 1}}),
	)
	if err != nil {
		return newMongoUnavailableErrorf(err, "failed to read shard bucket fences: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	count := int32(0)
	for cursor.Next(ctx) {
		var fence bucketFenceDocument
		if err := cursor.Decode(&fence); err != nil {
			return newMongoUnavailableErrorf(err, "failed to decode shard bucket fence: %v", err)
		}
		if fence.ShardID != owner.ShardID || fence.StorageBucket != count || fence.RangeID != owner.RangeID {
			return serviceerror.NewUnavailablef("shard %d has inconsistent bucket fence %d", owner.ShardID, count)
		}
		count++
	}
	if err := cursor.Err(); err != nil {
		return newMongoUnavailableErrorf(err, "failed to read shard bucket fences: %v", err)
	}
	if count != mongodbschema.StorageBucketCount {
		return serviceerror.NewUnavailablef("shard %d has %d bucket fences; expected %d", owner.ShardID, count, mongodbschema.StorageBucketCount)
	}
	return nil
}

func assertBucketRangeID(ctx context.Context, db client.Database, shardID, bucket int32, rangeID int64) error {
	if bucket < 0 || bucket >= mongodbschema.StorageBucketCount {
		return serviceerror.NewInvalidArgument("invalid storage bucket")
	}
	filter := bucketFenceFilter(shardID, bucket)
	filter["range_id"] = rangeID
	result, err := db.Collection(collectionHistoryRecords).UpdateOne(ctx, filter,
		bson.M{"$set": bson.M{"transaction_lock_id": uuid.NewString()}},
	)
	if err != nil {
		return newMongoUnavailableErrorf(err, "failed to lock bucket fence: %v", err)
	}
	if result.MatchedCount != 1 {
		return &persistence.ShardOwnershipLostError{ShardID: shardID, Msg: fmt.Sprintf("bucket %d ownership lost; expected range %d", bucket, rangeID)}
	}
	return nil
}
