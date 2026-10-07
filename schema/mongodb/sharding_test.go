package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestShardedCollections(t *testing.T) {
	want := map[string]bson.D{
		"shards":                {{Key: "_id", Value: 1}},
		"executions":            {{Key: "shard_id", Value: 1}},
		"current_executions":    {{Key: "shard_id", Value: 1}},
		"history_branches":      {{Key: "shard_id", Value: 1}},
		"history_nodes":         {{Key: "shard_id", Value: 1}},
		"transfer_tasks":        {{Key: "shard_id", Value: 1}},
		"timer_tasks":           {{Key: "shard_id", Value: 1}},
		"replication_tasks":     {{Key: "shard_id", Value: 1}},
		"visibility_tasks":      {{Key: "shard_id", Value: 1}},
		"replication_dlq_tasks": {{Key: "shard_id", Value: 1}},
		"task_queues":           matchingShardKey(),
		"tasks":                 matchingShardKey(),
		"visibility": {
			{Key: "NamespaceId", Value: 1},
			{Key: "RunId", Value: "hashed"},
		},
	}

	got := ShardedCollections()
	require.Len(t, got, len(want))
	for _, collection := range got {
		require.Contains(t, want, collection.Name)
		require.Equal(t, want[collection.Name], collection.ShardKey)
		delete(want, collection.Name)
	}
	require.Empty(t, want)
}
