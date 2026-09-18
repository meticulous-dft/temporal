//go:build integration

package mongodb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

func TestCreateNamespaceRollsBackWhenMetadataBumpFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "nsc")
	storeValue, err := factory.NewMetadataStore()
	require.NoError(t, err)
	store, ok := storeValue.(*metadataStore)
	require.True(t, ok)

	metadataBefore, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	failingMetadata := &failOnceCollection{Collection: factory.database.Collection(collectionNamespaceMetadata)}
	store.db = &collectionOverrideDatabase{
		Database:   factory.database,
		name:       collectionNamespaceMetadata,
		collection: failingMetadata,
	}
	failingMetadata.failNext.Store(true)

	request := &persistence.InternalCreateNamespaceRequest{
		ID:        "namespace-id",
		Name:      "namespace-name",
		Namespace: persistence.NewDataBlob([]byte("namespace"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}
	_, err = store.CreateNamespace(ctx, request)
	require.ErrorContains(t, err, "injected permanent failure")

	count, err := factory.database.Collection(collectionNamespaces).CountDocuments(ctx, bson.M{"_id": request.ID})
	require.NoError(t, err)
	require.Zero(t, count)
	metadataAfterFailure, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, metadataBefore.NotificationVersion, metadataAfterFailure.NotificationVersion)

	_, err = store.CreateNamespace(ctx, request)
	require.NoError(t, err)
	count, err = factory.database.Collection(collectionNamespaces).CountDocuments(ctx, bson.M{"_id": request.ID})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestUpdateNamespaceRollsBackWhenMetadataBumpFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "nsu")
	storeValue, err := factory.NewMetadataStore()
	require.NoError(t, err)
	store, ok := storeValue.(*metadataStore)
	require.True(t, ok)

	request := &persistence.InternalCreateNamespaceRequest{
		ID:        "namespace-id",
		Name:      "namespace-name",
		Namespace: persistence.NewDataBlob([]byte("before"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}
	_, err = store.CreateNamespace(ctx, request)
	require.NoError(t, err)
	metadataBefore, err := store.GetMetadata(ctx)
	require.NoError(t, err)

	failingMetadata := &failOnceCollection{Collection: factory.database.Collection(collectionNamespaceMetadata)}
	store.db = &collectionOverrideDatabase{
		Database:   factory.database,
		name:       collectionNamespaceMetadata,
		collection: failingMetadata,
	}
	failingMetadata.failNext.Store(true)
	updateRequest := &persistence.InternalUpdateNamespaceRequest{
		Id:                  request.ID,
		Name:                request.Name,
		Namespace:           persistence.NewDataBlob([]byte("after"), enumspb.ENCODING_TYPE_PROTO3.String()),
		NotificationVersion: metadataBefore.NotificationVersion,
	}
	err = store.UpdateNamespace(ctx, updateRequest)
	require.ErrorContains(t, err, "injected permanent failure")

	namespaceAfterFailure, err := store.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: request.ID})
	require.NoError(t, err)
	require.Equal(t, []byte("before"), namespaceAfterFailure.Namespace.Data)
	metadataAfterFailure, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, metadataBefore.NotificationVersion, metadataAfterFailure.NotificationVersion)

	require.NoError(t, store.UpdateNamespace(ctx, updateRequest))
	namespaceAfterRetry, err := store.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: request.ID})
	require.NoError(t, err)
	require.Equal(t, []byte("after"), namespaceAfterRetry.Namespace.Data)

	metadataBeforeRename, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	failingMetadata.failNext.Store(true)
	renameRequest := &persistence.InternalRenameNamespaceRequest{
		InternalUpdateNamespaceRequest: &persistence.InternalUpdateNamespaceRequest{
			Id:                  request.ID,
			Name:                "renamed-namespace",
			Namespace:           updateRequest.Namespace,
			NotificationVersion: metadataBeforeRename.NotificationVersion,
		},
		PreviousName: request.Name,
	}
	err = store.RenameNamespace(ctx, renameRequest)
	require.ErrorContains(t, err, "injected permanent failure")
	_, err = store.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: request.Name})
	require.NoError(t, err)
	_, err = store.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: renameRequest.Name})
	var notFound *serviceerror.NamespaceNotFound
	require.ErrorAs(t, err, &notFound)
	metadataAfterRenameFailure, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, metadataBeforeRename.NotificationVersion, metadataAfterRenameFailure.NotificationVersion)

	require.NoError(t, store.RenameNamespace(ctx, renameRequest))
	_, err = store.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: renameRequest.Name})
	require.NoError(t, err)
}

func TestConcurrentNamespaceUpdatesCommitExactlyOneVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "nscas")
	store, err := factory.NewMetadataStore()
	require.NoError(t, err)
	createRequest := &persistence.InternalCreateNamespaceRequest{
		ID:        "namespace-id",
		Name:      "namespace-name",
		Namespace: persistence.NewDataBlob([]byte("initial"), enumspb.ENCODING_TYPE_PROTO3.String()),
	}
	_, err = store.CreateNamespace(ctx, createRequest)
	require.NoError(t, err)
	metadataBefore, err := store.GetMetadata(ctx)
	require.NoError(t, err)

	type updateResult struct {
		data []byte
		err  error
	}
	start := make(chan struct{})
	results := make(chan updateResult, 2)
	for _, data := range [][]byte{[]byte("update-one"), []byte("update-two")} {
		data := data
		go func() {
			<-start
			err := store.UpdateNamespace(ctx, &persistence.InternalUpdateNamespaceRequest{
				Id:                  createRequest.ID,
				Name:                createRequest.Name,
				Namespace:           persistence.NewDataBlob(data, enumspb.ENCODING_TYPE_PROTO3.String()),
				NotificationVersion: metadataBefore.NotificationVersion,
			})
			results <- updateResult{data: data, err: err}
		}()
	}
	close(start)

	var successfulData []byte
	failures := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			successfulData = result.data
		} else {
			failures++
		}
	}
	require.NotNil(t, successfulData)
	require.Equal(t, 1, failures)

	namespaceAfter, err := store.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: createRequest.ID})
	require.NoError(t, err)
	require.Equal(t, successfulData, namespaceAfter.Namespace.Data)
	metadataAfter, err := store.GetMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, metadataBefore.NotificationVersion+1, metadataAfter.NotificationVersion)
}

func TestAppendFirstHistoryNodeRollsBackWhenBranchWriteFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "hist")
	storeValue, err := factory.NewExecutionStore()
	require.NoError(t, err)
	store, ok := storeValue.(*executionStore)
	require.True(t, ok)

	failingBranches := &failOnceCollection{Collection: factory.database.Collection(collectionHistoryBranches)}
	failingBranches.failNext.Store(true)
	store.historyBranchesCol = failingBranches
	request := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		IsNewBranch: true,
		BranchInfo: &persistencespb.HistoryBranch{
			TreeId:   "tree-id",
			BranchId: "branch-id",
		},
		TreeInfo: persistence.NewDataBlob([]byte("tree"), enumspb.ENCODING_TYPE_PROTO3.String()),
		Node: persistence.InternalHistoryNode{
			NodeID:        1,
			TransactionID: 1,
			Events:        persistence.NewDataBlob([]byte("events"), enumspb.ENCODING_TYPE_PROTO3.String()),
		},
	}
	err = store.AppendHistoryNodes(ctx, request)
	require.ErrorContains(t, err, "injected permanent failure")

	for _, collectionName := range []string{collectionHistoryNodes, collectionHistoryBranches} {
		count, err := factory.database.Collection(collectionName).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Zero(t, count, "partial history write remained in %s", collectionName)
	}

	require.NoError(t, store.AppendHistoryNodes(ctx, request))
	for _, collectionName := range []string{collectionHistoryNodes, collectionHistoryBranches} {
		count, err := factory.database.Collection(collectionName).CountDocuments(ctx, bson.M{})
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	}
}

func TestConcurrentQueueRangeDeletesDoNotRegressMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	factory, _ := newFencingTestFactory(t, "qrd")
	storeValue, err := factory.NewQueueV2()
	require.NoError(t, err)
	store, ok := storeValue.(*queueV2Store)
	require.True(t, ok)

	queueType := persistence.QueueV2Type(1)
	queueName := "queue"
	_, err = store.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{QueueType: queueType, QueueName: queueName})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		_, err = store.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
			QueueType: queueType,
			QueueName: queueName,
			Blob:      persistence.NewDataBlob([]byte{byte(i)}, enumspb.ENCODING_TYPE_PROTO3.String()),
		})
		require.NoError(t, err)
	}

	pause := newFencingPause()
	t.Cleanup(pause.release)
	store.metadataCol = &pauseOnMarkedFindCollection{
		Collection: factory.database.Collection(collectionQueueV2Metadata),
		pause:      pause,
	}
	lowDeleteDone := make(chan error, 1)
	lowCtx := context.WithValue(ctx, queueRangeDeletePauseKey{}, true)
	go func() {
		_, err := store.RangeDeleteMessages(lowCtx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType,
			QueueName: queueName,
			InclusiveMaxMessageMetadata: persistence.MessageMetadata{
				ID: persistence.FirstQueueMessageID + 1,
			},
		})
		lowDeleteDone <- err
	}()

	select {
	case <-pause.reached:
	case <-ctx.Done():
		t.Fatal("timed out waiting for lower range delete to load metadata")
	}
	_, err = store.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
		QueueType: queueType,
		QueueName: queueName,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: persistence.FirstQueueMessageID + 3,
		},
	})
	require.NoError(t, err)
	pause.release()
	select {
	case err = <-lowDeleteDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for lower range delete to finish")
	}

	var metadata queueV2MetadataDocument
	err = factory.database.Collection(collectionQueueV2Metadata).
		FindOne(ctx, bson.M{"_id": queueV2MetadataDocID(queueType, queueName)}).
		Decode(&metadata)
	require.NoError(t, err)
	queueProto, err := store.extractQueueProto(&metadata)
	require.NoError(t, err)
	require.EqualValues(
		t,
		persistence.FirstQueueMessageID+4,
		queueProto.Partitions[defaultQueueV2Partition].MinMessageId,
	)
	count, err := factory.database.Collection(collectionQueueV2Messages).CountDocuments(ctx, bson.M{
		"queue_type": int32(queueType),
		"queue_name": queueName,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

type collectionOverrideDatabase struct {
	client.Database
	name       string
	collection client.Collection
}

func (d *collectionOverrideDatabase) Collection(name string) client.Collection {
	if name == d.name {
		return d.collection
	}
	return d.Database.Collection(name)
}

type failOnceCollection struct {
	client.Collection
	failNext atomic.Bool
}

func (c *failOnceCollection) UpdateOne(
	ctx context.Context,
	filter interface{},
	update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	if c.failNext.CompareAndSwap(true, false) {
		return nil, errors.New("injected permanent failure")
	}
	return c.Collection.UpdateOne(ctx, filter, update, opts...)
}

type queueRangeDeletePauseKey struct{}

type pauseOnMarkedFindCollection struct {
	client.Collection
	pause *fencingPause
}

func (c *pauseOnMarkedFindCollection) FindOne(
	ctx context.Context,
	filter interface{},
	opts ...*options.FindOneOptions,
) client.SingleResult {
	result := c.Collection.FindOne(ctx, filter, opts...)
	marked, _ := ctx.Value(queueRangeDeletePauseKey{}).(bool)
	if !marked {
		return result
	}
	return &shardFenceBlockingSingleResult{
		SingleResult: result,
		ctx:          ctx,
		pause:        c.pause,
	}
}
