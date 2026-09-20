//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	persistencemongodb "go.temporal.io/server/common/persistence/mongodb"
	dbschemas "go.temporal.io/server/schema"
	mongodbschema "go.temporal.io/server/schema/mongodb"
	"go.temporal.io/server/temporal/environment"
)

func TestSchemaLifecycleAndCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	cfg := config.MongoDB{
		Hosts:          schemaIntegrationTestHosts(),
		User:           "temporal",
		Password:       "temporal",
		AuthSource:     "admin",
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		DatabaseName:   fmt.Sprintf("temporal_test_schema_%d", time.Now().UnixNano()),
		ConnectTimeout: 10 * time.Second,
	}
	connection, err := NewConnection(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := connection.database.Drop(cleanupCtx); err != nil {
			t.Errorf("drop MongoDB schema test database: %v", err)
		}
		if err := connection.Close(cleanupCtx); err != nil {
			t.Errorf("close MongoDB schema test connection: %v", err)
		}
	})

	_, err = connection.ReadSchemaVersion(ctx)
	require.ErrorIs(t, err, ErrNoSchemaVersion)
	require.NoError(t, connection.SetupSchema(ctx, "0.0"))

	version, err := connection.ReadSchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, VersionInfo{CurrentVersion: "0.0.0", MinCompatibleVersion: "0.0.0"}, version)

	updater := NewUpdater(connection)
	require.NoError(t, updater.Run(ctx, dbschemas.Assets(), "mongodb/temporal/versioned", ""))
	require.NoError(t, updater.Run(ctx, dbschemas.Assets(), "mongodb/temporal/versioned", ""))
	require.NoError(t, NewShardingInstaller(connection).Run(ctx))

	version, err = connection.ReadSchemaVersion(ctx)
	require.NoError(t, err)
	require.Equal(t, "1.1.0", version.CurrentVersion)
	require.Equal(t, "1.0", version.MinCompatibleVersion)

	historyCount, err := connection.database.Collection(mongodbschema.SchemaUpdateHistoryCollection).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(2), historyCount)
	requireCollectionIndex(ctx, t, connection.database, "tasks", "tasks_ttl")
	requireCollectionIndex(ctx, t, connection.database, "visibility", "visibility_default")
	requireCollectionIndex(ctx, t, connection.database, "visibility", "visibility_search_attributes")

	require.NoError(t, persistencemongodb.CheckCompatibleVersion(cfg, log.NewTestLogger()))
	setSchemaVersion(ctx, t, connection.database, cfg.DatabaseName, "0.9", "0.9")
	require.ErrorContains(t, persistencemongodb.CheckCompatibleVersion(cfg, log.NewTestLogger()), "server requires at least 1.1.0")
	setSchemaVersion(ctx, t, connection.database, cfg.DatabaseName, "1.1", "1.2")
	require.ErrorContains(t, persistencemongodb.CheckCompatibleVersion(cfg, log.NewTestLogger()), "requires server schema compatibility 1.2.0")
}

func requireCollectionIndex(ctx context.Context, t *testing.T, database *mongo.Database, collection, index string) {
	t.Helper()
	cursor, err := database.Collection(collection).Indexes().List(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, cursor.Close(ctx)) }()

	for cursor.Next(ctx) {
		var definition struct {
			Name string `bson:"name"`
		}
		require.NoError(t, cursor.Decode(&definition))
		if definition.Name == index {
			return
		}
	}
	require.NoError(t, cursor.Err())
	t.Fatalf("index %q was not created on collection %q", index, collection)
}

func setSchemaVersion(
	ctx context.Context,
	t *testing.T,
	database *mongo.Database,
	databaseName string,
	currentVersion string,
	minCompatibleVersion string,
) {
	t.Helper()
	_, err := database.Collection(mongodbschema.SchemaVersionCollection).UpdateOne(
		ctx,
		bson.M{"_id": databaseName},
		bson.M{"$set": bson.M{
			"curr_version":           currentVersion,
			"min_compatible_version": minCompatibleVersion,
		}},
	)
	require.NoError(t, err)
}

func schemaIntegrationTestHosts() []string {
	if seeds := os.Getenv("MONGODB_SEEDS"); seeds != "" {
		var hosts []string
		for _, seed := range strings.Split(seeds, ",") {
			if seed = strings.TrimSpace(seed); seed != "" {
				hosts = append(hosts, seed)
			}
		}
		if len(hosts) > 0 {
			return hosts
		}
	}
	return []string{net.JoinHostPort(environment.GetMongoDBAddress(), strconv.Itoa(environment.GetMongoDBPort()))}
}
