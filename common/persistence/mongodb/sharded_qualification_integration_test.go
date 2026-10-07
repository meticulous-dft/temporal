//go:build integration

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

const (
	shardedQualificationTaskCount = 50
	shardedRetryInterval          = 100 * time.Millisecond
)

func TestShardedSchemaContractAndRestart(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "sharded_schema_contract")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	requireShardedTopology(t, factory)

	var version schemaVersionDocument
	require.NoError(t, factory.database.Collection(mongodbschema.SchemaVersionCollection).
		FindOne(ctx, bson.M{"_id": cfg.DatabaseName}).Decode(&version))
	require.Equal(t, mongodbschema.TopologySharded, version.Topology)
	require.Equal(t, mongodbschema.ShardingPlanVersion, version.ShardingPlanVersion)

	for _, expected := range mongodbschema.ShardedCollections() {
		var installed struct {
			Key bson.D `bson:"key"`
		}
		require.NoError(t, factory.client.Database("config").Collection("collections").
			FindOne(ctx, bson.M{"_id": cfg.DatabaseName + "." + expected.Name}).Decode(&installed))
		require.Equal(t, shardKeyFields(expected.ShardKey), shardKeyFields(installed.Key), expected.Name)
	}

	_, err := factory.database.Collection(mongodbschema.SchemaVersionCollection).UpdateOne(
		ctx,
		bson.M{"_id": cfg.DatabaseName},
		bson.M{"$set": bson.M{"sharding_plan_version": "invalid"}},
	)
	require.NoError(t, err)
	_, err = NewFactory(cfg, "test-cluster", log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, `requires sharding plan version "1"`)

	_, err = factory.database.Collection(mongodbschema.SchemaVersionCollection).UpdateOne(
		ctx,
		bson.M{"_id": cfg.DatabaseName},
		bson.M{"$set": bson.M{"sharding_plan_version": mongodbschema.ShardingPlanVersion}},
	)
	require.NoError(t, err)
	restartedFactory, err := NewFactory(cfg, "test-cluster", log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	restartedFactory.Close()
}

func TestShardedHotPathQueriesTargetOneShard(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "sharded_targeting")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	requireShardedTopology(t, factory)

	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "split", Value: cfg.DatabaseName + ".executions"},
		{Key: "middle", Value: bson.D{{Key: "shard_id", Value: int32(0)}}},
	}))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: cfg.DatabaseName + ".executions"},
		{Key: "find", Value: bson.D{{Key: "shard_id", Value: int32(-1)}}},
		{Key: "to", Value: "shard-rs-0"},
	}))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: cfg.DatabaseName + ".executions"},
		{Key: "find", Value: bson.D{{Key: "shard_id", Value: int32(1)}}},
		{Key: "to", Value: "shard-rs-1"},
	}))

	matchingBoundary := bson.D{
		{Key: "namespace_id", Value: "m"},
		{Key: "task_queue", Value: primitive.MinKey{}},
		{Key: "task_type", Value: primitive.MinKey{}},
		{Key: "subqueue", Value: primitive.MinKey{}},
	}
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "split", Value: cfg.DatabaseName + ".tasks"},
		{Key: "middle", Value: matchingBoundary},
	}))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: cfg.DatabaseName + ".tasks"},
		{Key: "find", Value: matchingRoute("a", "queue")},
		{Key: "to", Value: "shard-rs-0"},
	}))
	require.NoError(t, runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: cfg.DatabaseName + ".tasks"},
		{Key: "find", Value: matchingRoute("z", "queue")},
		{Key: "to", Value: "shard-rs-1"},
	}))

	require.Equal(t, 1, explainShardCount(ctx, t, factory, collectionExecutions, bson.D{{Key: "shard_id", Value: int32(1)}}))
	require.Equal(t, 2, explainShardCount(ctx, t, factory, collectionExecutions, bson.D{{Key: "namespace_id", Value: "missing-route"}}))
	require.Equal(t, 1, explainShardCount(ctx, t, factory, collectionTasks, matchingRoute("z", "queue")))
	require.Equal(t, 2, explainShardCount(ctx, t, factory, collectionTasks, bson.D{{Key: "task_queue", Value: "queue"}}))
}

func TestCreateTasksSurvivesChunkMigration(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "sharded_chunk_migration")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	requireShardedTopology(t, factory)
	store, taskQueueInfo := newShardedTaskQueue(ctx, t, factory, "z", "migration-queue")

	progress, workloadDone := runShardedTaskWorkload(ctx, factory, store, taskQueueInfo, "z", "migration-queue", shardedQualificationTaskCount)
	completed := waitForShardedProgress(ctx, t, progress, workloadDone, 0, 10)
	require.NoError(t, moveMatchingChunk(ctx, factory, cfg.DatabaseName, "z", "migration-queue", "shard-rs-1"))
	completed = waitForShardedProgress(ctx, t, progress, workloadDone, completed, 30)
	require.NoError(t, moveMatchingChunk(ctx, factory, cfg.DatabaseName, "z", "migration-queue", "shard-rs-0"))
	waitForShardedProgress(ctx, t, progress, workloadDone, completed, shardedQualificationTaskCount)
	require.NoError(t, <-workloadDone)
	requireShardedTaskCount(ctx, t, factory, "z", "migration-queue", shardedQualificationTaskCount)
}

func TestCreateTasksSurvivesShardPrimaryStepdown(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "sharded_stepdown")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	requireShardedTopology(t, factory)
	store, taskQueueInfo := newShardedTaskQueue(ctx, t, factory, "a", "stepdown-queue")

	pause := newFencingPause()
	t.Cleanup(pause.release)
	store.db = &pauseTaskInsertDatabase{Database: factory.database, pause: pause}
	transactionDone := make(chan error, 1)
	go func() {
		_, err := store.CreateTasks(ctx, newShardedCreateTasksRequest(taskQueueInfo, "a", "stepdown-queue", 1))
		transactionDone <- err
	}()

	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the transaction to reach the shard stepdown boundary")
	}
	stepDownShardPrimary(ctx, t, cfg, "shard-rs-0", []string{"127.0.0.1:27201", "127.0.0.1:27202", "127.0.0.1:27203"})
	pause.release()
	require.NoError(t, <-transactionDone)
	requireShardedTaskCount(ctx, t, factory, "a", "stepdown-queue", 1)
}

func TestCreateTasksSurvivesMongosRestart(t *testing.T) {
	factory, cfg := newFencingTestFactory(t, "sharded_mongos_restart")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	requireShardedTopology(t, factory)
	store, taskQueueInfo := newShardedTaskQueue(ctx, t, factory, "a", "mongos-restart-queue")

	progress, workloadDone := runShardedTaskWorkload(ctx, factory, store, taskQueueInfo, "a", "mongos-restart-queue", shardedQualificationTaskCount)
	completed := waitForShardedProgress(ctx, t, progress, workloadDone, 0, 10)
	restartMongos(ctx, t, cfg, "127.0.0.1:27017")
	waitForShardedProgress(ctx, t, progress, workloadDone, completed, shardedQualificationTaskCount)
	require.NoError(t, <-workloadDone)
	requireShardedTaskCount(ctx, t, factory, "a", "mongos-restart-queue", shardedQualificationTaskCount)
}

func requireShardedTopology(t *testing.T, factory *Factory) {
	t.Helper()
	if factory.topologyInfo.Type != client.TopologySharded {
		t.Skipf("sharded cluster required; connected topology is %s", factory.topologyInfo.Type.String())
	}
}

func shardKeyFields(key bson.D) []string {
	fields := make([]string, 0, len(key))
	for _, field := range key {
		fields = append(fields, fmt.Sprintf("%s:%v", field.Key, field.Value))
	}
	return fields
}

func runAdminCommand(ctx context.Context, factory *Factory, command bson.D) error {
	return factory.client.Database("admin").RunCommand(ctx, command).Err()
}

func matchingRoute(namespaceID, taskQueue string) bson.D {
	return bson.D{
		{Key: "namespace_id", Value: namespaceID},
		{Key: "task_queue", Value: taskQueue},
		{Key: "task_type", Value: int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)},
		{Key: "subqueue", Value: persistence.SubqueueZero},
	}
}

func explainShardCount(
	ctx context.Context,
	t *testing.T,
	factory *Factory,
	collection string,
	filter bson.D,
) int {
	t.Helper()
	var result bson.M
	require.NoError(t, factory.database.RunCommand(ctx, bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: collection},
			{Key: "filter", Value: filter},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}).Decode(&result))
	count := maxExplainShardCount(result)
	require.Positive(t, count, "explain did not report targeted shards: %#v", result)
	return count
}

func maxExplainShardCount(value interface{}) int {
	maximum := 0
	switch value := value.(type) {
	case bson.M:
		for key, child := range value {
			if key == "shards" {
				if shards, ok := child.(bson.A); ok && len(shards) > maximum {
					maximum = len(shards)
				}
			}
			if count := maxExplainShardCount(child); count > maximum {
				maximum = count
			}
		}
	case bson.D:
		for _, child := range value {
			if child.Key == "shards" {
				if shards, ok := child.Value.(bson.A); ok && len(shards) > maximum {
					maximum = len(shards)
				}
			}
			if count := maxExplainShardCount(child.Value); count > maximum {
				maximum = count
			}
		}
	case bson.A:
		for _, child := range value {
			if count := maxExplainShardCount(child); count > maximum {
				maximum = count
			}
		}
	default:
	}
	return maximum
}

func newShardedTaskQueue(
	ctx context.Context,
	t *testing.T,
	factory *Factory,
	namespaceID string,
	taskQueue string,
) (*taskStore, *commonpb.DataBlob) {
	t.Helper()
	storeValue, err := factory.NewTaskStore()
	require.NoError(t, err)
	store, ok := storeValue.(*taskStore)
	require.True(t, ok)
	taskQueueInfo := persistence.NewDataBlob([]byte("task-queue"), enumspb.ENCODING_TYPE_PROTO3.String())
	require.NoError(t, store.CreateTaskQueue(ctx, &persistence.InternalCreateTaskQueueRequest{
		NamespaceID:   namespaceID,
		TaskQueue:     taskQueue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
	}))
	return store, taskQueueInfo
}

func newShardedCreateTasksRequest(
	taskQueueInfo *commonpb.DataBlob,
	namespaceID string,
	taskQueue string,
	taskID int64,
) *persistence.InternalCreateTasksRequest {
	return &persistence.InternalCreateTasksRequest{
		NamespaceID:   namespaceID,
		TaskQueue:     taskQueue,
		TaskType:      enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		RangeID:       1,
		TaskQueueInfo: taskQueueInfo,
		Tasks: []*persistence.InternalCreateTask{{
			TaskId:   taskID,
			Task:     persistence.NewDataBlob([]byte(fmt.Sprintf("task-%d", taskID)), enumspb.ENCODING_TYPE_PROTO3.String()),
			Subqueue: persistence.SubqueueZero,
		}},
	}
}

func runShardedTaskWorkload(
	ctx context.Context,
	factory *Factory,
	store *taskStore,
	taskQueueInfo *commonpb.DataBlob,
	namespaceID string,
	taskQueue string,
	taskCount int,
) (<-chan struct{}, <-chan error) {
	progress := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		defer close(progress)
		for taskID := 1; taskID <= taskCount; taskID++ {
			if err := createShardedTaskWithRetry(ctx, factory, store, taskQueueInfo, namespaceID, taskQueue, int64(taskID)); err != nil {
				done <- err
				return
			}
			progress <- struct{}{}
		}
		done <- nil
	}()
	return progress, done
}

func createShardedTaskWithRetry(
	ctx context.Context,
	factory *Factory,
	store *taskStore,
	taskQueueInfo *commonpb.DataBlob,
	namespaceID string,
	taskQueue string,
	taskID int64,
) error {
	for {
		_, err := store.CreateTasks(ctx, newShardedCreateTasksRequest(taskQueueInfo, namespaceID, taskQueue, taskID))
		if err == nil {
			return nil
		}
		count, countErr := factory.database.Collection(collectionTasks).CountDocuments(ctx, bson.M{
			"namespace_id": namespaceID,
			"task_queue":   taskQueue,
			"task_id":      taskID,
		})
		if countErr == nil && count == 1 {
			return nil
		}
		var unavailable *serviceerror.Unavailable
		if !errors.As(err, &unavailable) &&
			!mongoErrorHasLabel(err, "RetryableWriteError") &&
			!mongoErrorHasLabel(err, "TransientTransactionError") &&
			!mongoErrorHasLabel(err, "UnknownTransactionCommitResult") {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(shardedRetryInterval):
		}
	}
}

func waitForShardedProgress(
	ctx context.Context,
	t *testing.T,
	progress <-chan struct{},
	done <-chan error,
	completed int,
	target int,
) int {
	t.Helper()
	for completed < target {
		select {
		case _, ok := <-progress:
			require.True(t, ok, "workload stopped after %d of %d tasks", completed, target)
			completed++
		case err := <-done:
			require.NoError(t, err)
			t.Fatalf("workload stopped after %d of %d tasks", completed, target)
		case <-ctx.Done():
			t.Fatalf("workload reached %d of %d tasks before timeout", completed, target)
		}
	}
	return completed
}

func requireShardedTaskCount(
	ctx context.Context,
	t *testing.T,
	factory *Factory,
	namespaceID string,
	taskQueue string,
	expected int,
) {
	t.Helper()
	count, err := factory.database.Collection(collectionTasks).CountDocuments(ctx, bson.M{
		"namespace_id": namespaceID,
		"task_queue":   taskQueue,
	})
	require.NoError(t, err)
	require.Equal(t, int64(expected), count)
}

func moveMatchingChunk(
	ctx context.Context,
	factory *Factory,
	databaseName string,
	namespaceID string,
	taskQueue string,
	destination string,
) error {
	return runAdminCommand(ctx, factory, bson.D{
		{Key: "moveChunk", Value: databaseName + ".tasks"},
		{Key: "find", Value: matchingRoute(namespaceID, taskQueue)},
		{Key: "to", Value: destination},
		{Key: "_waitForDelete", Value: true},
	})
}

type pauseTaskInsertDatabase struct {
	client.Database
	pause *fencingPause
}

func (d *pauseTaskInsertDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name != collectionTasks {
		return collection
	}
	return &pauseTaskInsertCollection{Collection: collection, pause: d.pause}
}

type pauseTaskInsertCollection struct {
	client.Collection
	pause *fencingPause
}

func (c *pauseTaskInsertCollection) InsertMany(
	ctx context.Context,
	documents []interface{},
	opts ...*options.InsertManyOptions,
) (*mongo.InsertManyResult, error) {
	if err := c.pause.wait(ctx); err != nil {
		return nil, err
	}
	return c.Collection.InsertMany(ctx, documents, opts...)
}

func stepDownShardPrimary(
	ctx context.Context,
	t *testing.T,
	baseCfg config.MongoDB,
	replicaSet string,
	hosts []string,
) {
	t.Helper()
	clientCfg := baseCfg
	clientCfg.DatabaseName = "admin"
	clientCfg.Hosts = hosts
	clientCfg.ReplicaSet = replicaSet
	directClient := newRawMongoClient(ctx, t, clientCfg, false)
	defer func() { require.NoError(t, directClient.Disconnect(context.Background())) }()

	oldPrimary := replicaSetPrimary(ctx, t, directClient)
	_ = directClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "replSetStepDown", Value: 30},
		{Key: "force", Value: true},
	}).Err()
	require.Eventually(t, func() bool {
		statusCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var status replicaSetStatus
		if err := directClient.Database("admin").RunCommand(statusCtx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status); err != nil {
			return false
		}
		return status.primary() != "" && status.primary() != oldPrimary
	}, 30*time.Second, shardedRetryInterval)
}

func replicaSetPrimary(ctx context.Context, t *testing.T, rawClient *mongo.Client) string {
	t.Helper()
	var status replicaSetStatus
	require.NoError(t, rawClient.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status))
	require.NotEmpty(t, status.primary())
	return status.primary()
}

func restartMongos(ctx context.Context, t *testing.T, baseCfg config.MongoDB, host string) {
	t.Helper()
	clientCfg := baseCfg
	clientCfg.DatabaseName = "admin"
	clientCfg.Hosts = []string{host}
	clientCfg.ReplicaSet = ""
	rawClient := newRawMongoClient(ctx, t, clientCfg, true)
	oldProcessID := mongosProcessID(ctx, t, rawClient)
	_ = rawClient.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shutdown", Value: 1},
		{Key: "force", Value: true},
	}).Err()
	require.NoError(t, rawClient.Disconnect(context.Background()))

	require.Eventually(t, func() bool {
		probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		probeClient, err := rawMongoClient(probeCtx, clientCfg, true)
		if err != nil {
			return false
		}
		defer func() { _ = probeClient.Disconnect(context.Background()) }()
		var hello struct {
			TopologyVersion struct {
				ProcessID primitive.ObjectID `bson:"processId"`
			} `bson:"topologyVersion"`
		}
		if err := probeClient.Database("admin").RunCommand(probeCtx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
			return false
		}
		return !hello.TopologyVersion.ProcessID.IsZero() && hello.TopologyVersion.ProcessID != oldProcessID
	}, 30*time.Second, shardedRetryInterval)
}

func mongosProcessID(ctx context.Context, t *testing.T, rawClient *mongo.Client) primitive.ObjectID {
	t.Helper()
	var hello struct {
		TopologyVersion struct {
			ProcessID primitive.ObjectID `bson:"processId"`
		} `bson:"topologyVersion"`
	}
	require.NoError(t, rawClient.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello))
	require.False(t, hello.TopologyVersion.ProcessID.IsZero())
	return hello.TopologyVersion.ProcessID
}

func newRawMongoClient(ctx context.Context, t *testing.T, cfg config.MongoDB, direct bool) *mongo.Client {
	t.Helper()
	rawClient, err := rawMongoClient(ctx, cfg, direct)
	require.NoError(t, err)
	return rawClient
}

func rawMongoClient(ctx context.Context, cfg config.MongoDB, direct bool) (*mongo.Client, error) {
	clientOptions, _, err := buildMongoOptions(cfg)
	if err != nil {
		return nil, err
	}
	clientOptions.SetDirect(direct)
	rawClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	if err := rawClient.Ping(ctx, nil); err != nil {
		_ = rawClient.Disconnect(context.Background())
		return nil, err
	}
	return rawClient, nil
}
