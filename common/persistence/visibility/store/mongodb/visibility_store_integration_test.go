//go:build integration

package mongodb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	persistencemongodb "go.temporal.io/server/common/persistence/mongodb"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/temporal/environment"
	mongodbschematool "go.temporal.io/server/tools/mongodb"
)

func TestMongoDBVisibilityLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	cfg := config.MongoDB{
		Hosts:          []string{fmt.Sprintf("%s:%d", environment.GetMongoDBAddress(), environment.GetMongoDBPort())},
		User:           "temporal",
		Password:       "temporal",
		AuthSource:     "admin",
		DatabaseName:   fmt.Sprintf("temporal_visibility_%d", time.Now().UnixNano()),
		ReplicaSet:     environment.GetMongoDBReplicaSet(),
		ConnectTimeout: 10 * time.Second,
		MaxConns:       10,
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}
	if os.Getenv("TEMPORAL_MONGODB_SHARDED_TEST") != "" {
		cfg.ReplicaSet = ""
	}
	clientOptions, _, err := persistencemongodb.BuildMongoOptions(cfg)
	require.NoError(t, err)
	cleanupClient, err := mongo.Connect(ctx, clientOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		require.NoError(t, cleanupClient.Database(cfg.DatabaseName).Drop(cleanupCtx))
		require.NoError(t, cleanupClient.Disconnect(cleanupCtx))
	})
	require.NoError(t, mongodbschematool.InstallDefaultSchema(ctx, cfg))
	if os.Getenv("TEMPORAL_MONGODB_SHARDED_TEST") != "" {
		var shardingMetadata struct {
			Key bson.D `bson:"key"`
		}
		require.NoError(t, cleanupClient.Database("config").Collection("collections").
			FindOne(ctx, bson.M{"_id": cfg.DatabaseName + "." + visibilityCollection}).Decode(&shardingMetadata))
		require.Equal(t, bson.D{
			{Key: sadefs.NamespaceID, Value: int32(1)},
			{Key: sadefs.RunID, Value: "hashed"},
		}, shardingMetadata.Key)
	}

	visibilityStore, err := NewVisibilityStore(
		cfg,
		searchattribute.NewTestProvider(),
		searchattribute.NewTestMapperProvider(&searchattribute.TestMapper{Namespace: "test-namespace"}),
		nil,
		log.NewTestLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(visibilityStore.Close)

	startTime := time.Now().UTC().Truncate(time.Millisecond)
	for index := range 3 {
		request := visibilityRequest(int64(index+1), fmt.Sprintf("workflow-%d", index), fmt.Sprintf("run-%d", index), startTime.Add(time.Duration(index)*time.Second))
		require.NoError(t, visibilityStore.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{
			InternalVisibilityRequestBase: request,
		}))
	}
	closed := visibilityRequest(10, "workflow-0", "run-0", startTime)
	closed.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	require.NoError(t, visibilityStore.RecordWorkflowExecutionClosed(ctx, &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: closed,
		CloseTime:                     startTime.Add(time.Minute),
		HistoryLength:                 7,
	}))
	stale := visibilityRequest(9, "stale-payload", "run-0", startTime)
	require.NoError(t, visibilityStore.UpsertWorkflowExecution(ctx, &store.InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: stale,
	}))

	getResponse, err := visibilityStore.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: "namespace-id",
		Namespace:   "test-namespace",
		RunID:       "run-0",
	})
	require.NoError(t, err)
	require.Equal(t, "workflow-0", getResponse.Execution.WorkflowID)
	require.Equal(t, int64(7), getResponse.Execution.HistoryLength)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, getResponse.Execution.Status)

	firstPage, err := visibilityStore.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID: "namespace-id",
		Namespace:   "test-namespace",
		PageSize:    2,
		Query:       `WorkflowType = "visibility-type"`,
	})
	require.NoError(t, err)
	require.Len(t, firstPage.Executions, 2)
	require.NotEmpty(t, firstPage.NextPageToken)
	secondPage, err := visibilityStore.ListWorkflowExecutions(ctx, &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   "namespace-id",
		Namespace:     "test-namespace",
		PageSize:      2,
		Query:         `WorkflowType = "visibility-type"`,
		NextPageToken: firstPage.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, secondPage.Executions, 1)

	textMatches, err := visibilityStore.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "test-namespace",
		Query:       `AliasForText01 = "hello mongodb"`,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), textMatches.Count)

	grouped, err := visibilityStore.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{
		NamespaceID: "namespace-id",
		Namespace:   "test-namespace",
		Query:       `GROUP BY ExecutionStatus`,
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), grouped.Count)
	require.Len(t, grouped.Groups, 2)

	require.NoError(t, visibilityStore.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: namespace.ID("namespace-id"),
		WorkflowID:  "workflow-0",
		RunID:       "run-0",
		TaskID:      11,
	}))
	require.NoError(t, visibilityStore.UpsertWorkflowExecution(ctx, &store.InternalUpsertWorkflowExecutionRequest{
		InternalVisibilityRequestBase: closed,
	}))
	_, err = visibilityStore.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{
		NamespaceID: "namespace-id",
		Namespace:   "test-namespace",
		RunID:       "run-0",
	})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
}

func visibilityRequest(taskID int64, workflowID string, runID string, startTime time.Time) *store.InternalVisibilityRequestBase {
	typeMap := searchattribute.TestNameTypeMap()
	searchAttributes, err := searchattribute.Encode(map[string]any{
		"Text01": "hello mongodb world",
		"Int01":  taskID,
	}, &typeMap)
	if err != nil {
		panic(err)
	}
	return &store.InternalVisibilityRequestBase{
		NamespaceID:      "namespace-id",
		WorkflowID:       workflowID,
		RunID:            runID,
		WorkflowTypeName: "visibility-type",
		StartTime:        startTime,
		ExecutionTime:    startTime,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		TaskID:           taskID,
		TaskQueue:        "visibility-queue",
		SearchAttributes: searchAttributes,
		RootWorkflowID:   workflowID,
		RootRunID:        runID,
	}
}
