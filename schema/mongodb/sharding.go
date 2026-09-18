package mongodb

import "go.mongodb.org/mongo-driver/bson"

const (
	TopologyReplicaSet = "replica_set"
	TopologySharded    = "sharded"

	ShardingPlanVersion = "2"
)

type ShardedCollection struct {
	Name     string
	ShardKey bson.D
}

func ShardedCollections() []ShardedCollection {
	return []ShardedCollection{
		{Name: "shards", ShardKey: bson.D{{Key: "_id", Value: 1}}},
		{Name: "executions", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "current_executions", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "history_branches", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "history_nodes", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "transfer_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "timer_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "replication_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "visibility_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "replication_dlq_tasks", ShardKey: bson.D{{Key: "shard_id", Value: 1}}},
		{Name: "task_queues", ShardKey: matchingShardKey()},
		{Name: "tasks", ShardKey: matchingShardKey()},
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
		{Key: "subqueue", Value: 1},
	}
}
