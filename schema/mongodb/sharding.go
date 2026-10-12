package mongodb

import "go.mongodb.org/mongo-driver/bson"

const (
	TopologyReplicaSet = "replica_set"
	TopologySharded    = "sharded"

	ShardingPlanVersion = "5"
	StorageModelVersion = "bucketed_atomic_v1"
	StorageBucketCount  = 64
)

type ShardedCollection struct {
	Name     string
	ShardKey bson.D
}

func ShardedCollections() []ShardedCollection {
	return []ShardedCollection{
		{Name: "shards", ShardKey: bson.D{{Key: "_id", Value: 1}}},
		{Name: "history_records", ShardKey: bson.D{{Key: "shard_id", Value: 1}, {Key: "storage_bucket", Value: 1}}},
		{Name: "history_branches", ShardKey: bson.D{{Key: "tree_id", Value: 1}}},
		{Name: "history_nodes", ShardKey: bson.D{{Key: "tree_id", Value: 1}}},
		{Name: "replication_dlq_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "matching_records", ShardKey: matchingShardKey()},
		{Name: "visibility", ShardKey: bson.D{
			{Key: "NamespaceId", Value: 1},
			{Key: "RunId", Value: "hashed"},
		}},
	}
}

func matchingShardKey() bson.D {
	return bson.D{
		{Key: "namespace_id", Value: 1},
		{Key: "task_queue", Value: 1},
		{Key: "task_type", Value: 1},
	}
}
