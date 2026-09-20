package mongodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestVisibilityDocumentRoundTrip(t *testing.T) {
	typeMap := searchattribute.TestNameTypeMap()
	searchAttributes, err := searchattribute.Encode(map[string]any{
		"Int01":         int64(42),
		"Text01":        "hello  mongodb world",
		"KeywordList01": []string{"one", "two"},
	}, &typeMap)
	require.NoError(t, err)
	startTime := time.Date(2026, 9, 18, 1, 2, 3, 4, time.UTC)
	closeTime := startTime.Add(time.Minute)
	parentWorkflowID := "parent-workflow"
	parentRunID := "parent-run"
	request := &store.InternalRecordWorkflowExecutionClosedRequest{
		InternalVisibilityRequestBase: &store.InternalVisibilityRequestBase{
			NamespaceID:      "namespace-id",
			WorkflowID:       "workflow-id",
			RunID:            "run-id",
			WorkflowTypeName: "workflow-type",
			StartTime:        startTime,
			ExecutionTime:    startTime.Add(time.Second),
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			TaskID:           123,
			Memo:             persistence.NewDataBlob([]byte("memo"), enumspb.ENCODING_TYPE_JSON.String()),
			TaskQueue:        "task-queue",
			SearchAttributes: searchAttributes,
			ParentWorkflowID: &parentWorkflowID,
			ParentRunID:      &parentRunID,
			RootWorkflowID:   "root-workflow",
			RootRunID:        "root-run",
		},
		CloseTime:            closeTime,
		HistoryLength:        10,
		HistorySizeBytes:     20,
		ExecutionDuration:    30 * time.Second,
		StateTransitionCount: 40,
	}
	visibilityStore := &VisibilityStore{
		indexName:                "temporal",
		searchAttributesProvider: searchattribute.NewTestProvider(),
	}

	document, err := visibilityStore.buildClosedDocument(request)
	require.NoError(t, err)
	require.Equal(t, "namespace-id\x00run-id", document[documentIDField])
	require.Equal(t, int64(123), document[versionField])
	require.Equal(t, closeTime.UnixNano(), document[closeTimeSortField])
	require.Equal(t, []string{"hello", "mongodb", "world"}, document[textTokensField].(bson.M)["Text01"])

	info, err := visibilityStore.parseDocument(document, nil)
	require.NoError(t, err)
	require.Equal(t, request.WorkflowID, info.WorkflowID)
	require.Equal(t, request.RunID, info.RunID)
	require.Equal(t, request.WorkflowTypeName, info.TypeName)
	require.Equal(t, request.StartTime, info.StartTime)
	require.Equal(t, request.ExecutionTime, info.ExecutionTime)
	require.Equal(t, request.CloseTime, info.CloseTime)
	require.Equal(t, request.ExecutionDuration, info.ExecutionDuration)
	require.Equal(t, request.HistoryLength, info.HistoryLength)
	require.Equal(t, request.HistorySizeBytes, info.HistorySizeBytes)
	require.Equal(t, request.StateTransitionCount, info.StateTransitionCount)
	require.Equal(t, request.Status, info.Status)
	require.Equal(t, request.TaskQueue, info.TaskQueue)
	require.Equal(t, request.Memo, info.Memo)
	require.Equal(t, parentWorkflowID, info.ParentWorkflowID)
	require.Equal(t, parentRunID, info.ParentRunID)
	require.Equal(t, request.RootWorkflowID, info.RootWorkflowID)
	require.Equal(t, request.RootRunID, info.RootRunID)

	decoded, err := searchattribute.Decode(info.SearchAttributes, &typeMap, false)
	require.NoError(t, err)
	require.Equal(t, int64(42), decoded["Int01"])
	require.Equal(t, "hello  mongodb world", decoded["Text01"])
	require.Equal(t, []string{"one", "two"}, decoded["KeywordList01"])
	require.NotContains(t, document, sadefs.VisibilityTaskKey)
}

func TestOpenVisibilityDocumentSortsBeforeClosedDescending(t *testing.T) {
	visibilityStore := &VisibilityStore{
		indexName:                "temporal",
		searchAttributesProvider: searchattribute.NewTestProvider(),
	}
	document, err := visibilityStore.buildDocument(&store.InternalVisibilityRequestBase{
		NamespaceID: "namespace-id",
		RunID:       "run-id",
		Status:      enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	})
	require.NoError(t, err)
	require.Equal(t, maxDatetime.UnixNano(), document[closeTimeSortField])
	require.NotContains(t, document, sadefs.CloseTime)
}

func TestTombstoneRetainsShardRoutingAndVersion(t *testing.T) {
	closeTime := time.Now().UTC()
	document := tombstoneDocument(&manager.VisibilityDeleteWorkflowExecutionRequest{
		NamespaceID: "namespace-id",
		WorkflowID:  "workflow-id",
		RunID:       "run-id",
		TaskID:      321,
		CloseTime:   &closeTime,
	})

	require.Equal(t, "namespace-id", document[sadefs.NamespaceID])
	require.Equal(t, "run-id", document[sadefs.RunID])
	require.Equal(t, int64(321), document[versionField])
	require.Equal(t, true, document[deletedField])
	require.NotContains(t, document, sadefs.CloseTime)
}
