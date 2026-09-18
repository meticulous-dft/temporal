//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	taskspkg "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/temporal/environment"
	mongodbschematool "go.temporal.io/server/tools/mongodb"
)

func TestExecutionStoreRejectsStaleShardOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "execution")

	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)

	const (
		shardID       = int32(41)
		oldRangeID    = int64(100)
		newRangeID    = int64(101)
		namespaceID   = "namespace"
		workflowID    = "workflow"
		runID         = "run"
		historyTreeID = "history-tree"
		historyBranch = "history-branch"
	)
	shardInfo := persistence.NewDataBlob([]byte("shard"), enumspb.ENCODING_TYPE_PROTO3.String())
	_, err = shardStore.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return oldRangeID, shardInfo, nil
		},
		LifecycleContext: ctx,
	})
	require.NoError(t, err)

	executionStoreValue, err := factory.NewExecutionStore()
	require.NoError(t, err)
	executionStore, ok := executionStoreValue.(*executionStore)
	require.True(t, ok)

	pause := newFencingPause()
	t.Cleanup(pause.release)
	executionStore.db = &shardFenceBlockingDatabase{
		Database: factory.database,
		pause:    pause,
	}

	workflowTask := persistence.InternalHistoryTask{
		Key:  taskspkg.NewImmediateKey(1),
		Blob: persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}
	createRequest := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID: shardID,
		RangeID: oldRangeID,
		Mode:    persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot: persistence.InternalWorkflowSnapshot{
			NamespaceID:     namespaceID,
			WorkflowID:      workflowID,
			RunID:           runID,
			ExecutionInfo:   &persistencespb.WorkflowExecutionInfo{},
			ExecutionState:  &persistencespb.WorkflowExecutionState{RunId: runID, CreateRequestId: "request", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
			NextEventID:     2,
			DBRecordVersion: 1,
			Tasks: map[taskspkg.Category][]persistence.InternalHistoryTask{
				taskspkg.CategoryTransfer: {workflowTask},
			},
		},
		NewWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{
			{
				ShardID:     shardID,
				IsNewBranch: true,
				BranchInfo: &persistencespb.HistoryBranch{
					TreeId:   historyTreeID,
					BranchId: historyBranch,
				},
				TreeInfo: persistence.NewDataBlob([]byte("tree"), enumspb.ENCODING_TYPE_PROTO3.String()),
				Node: persistence.InternalHistoryNode{
					NodeID:        1,
					TransactionID: 1,
					Events:        persistence.NewDataBlob([]byte("events"), enumspb.ENCODING_TYPE_PROTO3.String()),
				},
			},
		},
	}

	staleWriteDone := make(chan error, 1)
	go func() {
		_, err := executionStore.CreateWorkflowExecution(ctx, createRequest)
		staleWriteDone <- err
	}()

	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the old owner to reach the shard fence")
	}

	err = shardStore.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
		ShardID:         shardID,
		RangeID:         newRangeID,
		ShardInfo:       shardInfo,
		PreviousRangeID: oldRangeID,
	})
	require.NoError(t, err)
	pause.release()

	select {
	case err = <-staleWriteDone:
	case <-ctx.Done():
		t.Fatal("timed out waiting for the stale workflow write")
	}
	var ownershipLost *persistence.ShardOwnershipLostError
	require.ErrorAs(t, err, &ownershipLost)

	for _, collectionName := range []string{
		collectionExecutions,
		collectionCurrentExecutions,
		collectionHistoryBranches,
		collectionHistoryNodes,
		collectionTransferTasks,
	} {
		count, err := factory.database.Collection(collectionName).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Zero(t, count, "stale owner wrote to %s", collectionName)
	}
}

func TestAddHistoryTasksRejectsStaleShardOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "add_history_tasks")
	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)
	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)

	const (
		shardID    = int32(42)
		oldRangeID = int64(100)
		newRangeID = int64(101)
	)
	shardInfo := persistence.NewDataBlob([]byte("shard"), enumspb.ENCODING_TYPE_PROTO3.String())
	_, err = shardStore.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{
		ShardID: shardID,
		CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return oldRangeID, shardInfo, nil
		},
		LifecycleContext: ctx,
	})
	require.NoError(t, err)
	require.NoError(t, shardStore.UpdateShard(ctx, &persistence.InternalUpdateShardRequest{
		ShardID:         shardID,
		RangeID:         newRangeID,
		ShardInfo:       shardInfo,
		PreviousRangeID: oldRangeID,
	}))

	request := &persistence.InternalAddHistoryTasksRequest{
		ShardID: shardID,
		RangeID: oldRangeID,
		Tasks: map[taskspkg.Category][]persistence.InternalHistoryTask{
			taskspkg.CategoryTransfer: {
				{
					Key:  taskspkg.NewImmediateKey(1),
					Blob: persistence.NewDataBlob([]byte("task"), enumspb.ENCODING_TYPE_PROTO3.String()),
				},
			},
		},
	}
	err = executionStore.AddHistoryTasks(ctx, request)
	var ownershipLost *persistence.ShardOwnershipLostError
	require.ErrorAs(t, err, &ownershipLost)

	count, err := factory.database.Collection(collectionTransferTasks).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Zero(t, count)

	request.RangeID = newRangeID
	require.NoError(t, executionStore.AddHistoryTasks(ctx, request))
	count, err = factory.database.Collection(collectionTransferTasks).CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

type fencingPause struct {
	reached     chan struct{}
	resume      chan struct{}
	reachedOnce sync.Once
	releaseOnce sync.Once
}

func newFencingPause() *fencingPause {
	return &fencingPause{
		reached: make(chan struct{}),
		resume:  make(chan struct{}),
	}
}

func (p *fencingPause) wait(ctx context.Context) error {
	var waitErr error
	p.reachedOnce.Do(func() {
		close(p.reached)
		select {
		case <-p.resume:
		case <-ctx.Done():
			waitErr = ctx.Err()
		}
	})
	return waitErr
}

func (p *fencingPause) release() {
	p.releaseOnce.Do(func() {
		close(p.resume)
	})
}

type shardFenceBlockingDatabase struct {
	client.Database
	pause *fencingPause
}

func (d *shardFenceBlockingDatabase) Collection(name string) client.Collection {
	collection := d.Database.Collection(name)
	if name != collectionShards {
		return collection
	}
	return &shardFenceBlockingCollection{
		Collection: collection,
		pause:      d.pause,
	}
}

type shardFenceBlockingCollection struct {
	client.Collection
	pause *fencingPause
}

func (c *shardFenceBlockingCollection) FindOne(
	ctx context.Context,
	filter interface{},
	opts ...*options.FindOneOptions,
) client.SingleResult {
	return &shardFenceBlockingSingleResult{
		SingleResult: c.Collection.FindOne(ctx, filter, opts...),
		ctx:          ctx,
		pause:        c.pause,
	}
}

func (c *shardFenceBlockingCollection) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	if err := c.pause.wait(ctx); err != nil {
		return nil, err
	}
	return c.Collection.UpdateOne(ctx, filter, update, opts...)
}

type shardFenceBlockingSingleResult struct {
	client.SingleResult
	ctx   context.Context
	pause *fencingPause
}

func (r *shardFenceBlockingSingleResult) Decode(value interface{}) error {
	if err := r.SingleResult.Decode(value); err != nil {
		return err
	}
	return r.pause.wait(r.ctx)
}

func newFencingTestFactory(t *testing.T, name string) (*Factory, config.MongoDB) {
	databaseSuffix := fmt.Sprintf("_fencing_%d", time.Now().UnixNano())
	databasePrefix := "temporal_test_" + name
	if maximumPrefixLength := 63 - len(databaseSuffix); len(databasePrefix) > maximumPrefixLength {
		databasePrefix = databasePrefix[:maximumPrefixLength]
	}
	databaseName := databasePrefix + databaseSuffix
	cfg := config.MongoDB{
		Hosts:          mongoDBIntegrationTestHosts(),
		User:           "temporal",
		Password:       "temporal",
		AuthSource:     "admin",
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		DatabaseName:   databaseName,
		ConnectTimeout: 30 * time.Second,
		MaxConns:       10,
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}

	schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	require.NoError(t, mongodbschematool.InstallDefaultSchema(schemaCtx, cfg))
	schemaCancel()

	factory, err := NewFactory(cfg, "test-cluster", log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() {
		factory.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer dropCancel()
		cleanupOptions, _, err := buildMongoOptions(cfg)
		if err != nil {
			t.Errorf("failed to build MongoDB cleanup options for %q: %v", databaseName, err)
			return
		}
		cleanupClient, err := mongo.Connect(dropCtx, cleanupOptions)
		if err != nil {
			t.Errorf("failed to create MongoDB cleanup client for %q: %v", databaseName, err)
			return
		}
		defer func() { _ = cleanupClient.Disconnect(dropCtx) }()
		if err := cleanupClient.Database(databaseName).Drop(dropCtx); err != nil {
			t.Errorf("failed to drop MongoDB test database %q: %v", databaseName, err)
		}
	})

	return factory, cfg
}

func mongoDBIntegrationTestHosts() []string {
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
