package mongodb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

func TestShardingInstallerInstallsAndCommitsPlan(t *testing.T) {
	database := &fakeShardingDatabase{
		topology: mongodbschema.TopologySharded,
		installed: map[string]collectionSharding{
			"shards": {Sharded: true, ShardKey: bson.D{{Key: "_id", Value: int32(1)}}},
		},
	}

	err := NewShardingInstaller(database).Run(context.Background())
	require.NoError(t, err)
	require.Len(t, database.sharded, len(mongodbschema.ShardedCollections())-1)
	require.Equal(t, mongodbschema.TopologySharded, database.committedTopology)
	require.Equal(t, mongodbschema.ShardingPlanVersion, database.committedPlan)
}

func TestShardingInstallerRejectsPopulatedUnplannedCollection(t *testing.T) {
	database := &fakeShardingDatabase{
		topology: mongodbschema.TopologySharded,
		counts:   map[string]int64{"executions": 1},
	}

	err := NewShardingInstaller(database).Run(context.Background())
	require.ErrorContains(t, err, `collection "executions" contains 1 documents`)
	require.Empty(t, database.committedTopology)
}

func TestShardingInstallerRejectsWrongInstalledKey(t *testing.T) {
	database := &fakeShardingDatabase{
		topology: mongodbschema.TopologySharded,
		installed: map[string]collectionSharding{
			"executions": {Sharded: true, ShardKey: bson.D{{Key: "namespace_id", Value: 1}}},
		},
	}

	err := NewShardingInstaller(database).Run(context.Background())
	require.ErrorContains(t, err, `collection "executions" has shard key`)
	require.Empty(t, database.committedTopology)
}

func TestShardingInstallerRejectsWrongHashedKey(t *testing.T) {
	database := &fakeShardingDatabase{
		topology: mongodbschema.TopologySharded,
		installed: map[string]collectionSharding{
			"visibility": {
				Sharded: true,
				ShardKey: bson.D{
					{Key: "NamespaceId", Value: 1},
					{Key: "RunId", Value: "not-hashed"},
				},
			},
		},
	}

	err := NewShardingInstaller(database).Run(context.Background())
	require.ErrorContains(t, err, `collection "visibility" has shard key`)
	require.Empty(t, database.committedTopology)
}

func TestShardingInstallerDoesNotCommitPartialPlan(t *testing.T) {
	database := &fakeShardingDatabase{
		topology:       mongodbschema.TopologySharded,
		failCollection: "history_nodes",
	}

	err := NewShardingInstaller(database).Run(context.Background())
	require.ErrorContains(t, err, "injected shardCollection failure")
	require.Empty(t, database.committedTopology)
}

func TestShardingInstallerStampsReplicaSetWithoutShardingCollections(t *testing.T) {
	database := &fakeShardingDatabase{topology: mongodbschema.TopologyReplicaSet}

	err := NewShardingInstaller(database).Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, database.sharded)
	require.Equal(t, mongodbschema.TopologyReplicaSet, database.committedTopology)
	require.Empty(t, database.committedPlan)
}

type fakeShardingDatabase struct {
	topology          string
	installed         map[string]collectionSharding
	counts            map[string]int64
	failCollection    string
	sharded           []mongodbschema.ShardedCollection
	committedTopology string
	committedPlan     string
}

func (d *fakeShardingDatabase) Topology(context.Context) (string, error) {
	return d.topology, nil
}

func (d *fakeShardingDatabase) CollectionSharding(_ context.Context, name string) (collectionSharding, error) {
	return d.installed[name], nil
}

func (d *fakeShardingDatabase) CountCollection(_ context.Context, name string) (int64, error) {
	return d.counts[name], nil
}

func (d *fakeShardingDatabase) ShardCollection(_ context.Context, plan mongodbschema.ShardedCollection) error {
	if plan.Name == d.failCollection {
		return errors.New("injected shardCollection failure")
	}
	d.sharded = append(d.sharded, plan)
	return nil
}

func (d *fakeShardingDatabase) CommitTopology(_ context.Context, topology, planVersion string) error {
	d.committedTopology = topology
	d.committedPlan = planVersion
	return nil
}
