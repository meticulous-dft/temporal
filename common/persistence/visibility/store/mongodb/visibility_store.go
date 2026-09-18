package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	persistencemongodb "go.temporal.io/server/common/persistence/mongodb"
	mongoclient "go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

const (
	PersistenceName      = "mongodb"
	visibilityCollection = "visibility"
)

type VisibilityStore struct {
	client                         mongoclient.Client
	collection                     mongoclient.Collection
	indexName                      string
	searchAttributesProvider       searchattribute.Provider
	searchAttributesMapperProvider searchattribute.MapperProvider
	chasmRegistry                  *chasm.Registry
	logger                         log.Logger
}

var _ store.VisibilityStore = (*VisibilityStore)(nil)

func NewVisibilityStore(
	cfg config.MongoDB,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	chasmRegistry *chasm.Registry,
	logger log.Logger,
) (*VisibilityStore, error) {
	if err := persistencemongodb.CheckCompatibleVersion(cfg, logger); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clientOptions, sessionOptions, err := persistencemongodb.BuildMongoOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := mongoclient.NewClient(ctx, clientOptions, sessionOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB visibility client: %w", err)
	}
	initialized := false
	defer func() {
		if !initialized {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()
			_ = client.Close(closeCtx)
		}
	}()
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect MongoDB visibility client: %w", err)
	}
	visibilityStore := newVisibilityStore(
		client,
		client.Database(cfg.DatabaseName).Collection(visibilityCollection),
		cfg.DatabaseName,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		chasmRegistry,
		logger,
	)
	if err := visibilityStore.ensureIndexes(ctx); err != nil {
		return nil, err
	}
	initialized = true
	return visibilityStore, nil
}

func newVisibilityStore(
	client mongoclient.Client,
	collection mongoclient.Collection,
	indexName string,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	chasmRegistry *chasm.Registry,
	logger log.Logger,
) *VisibilityStore {
	return &VisibilityStore{
		client:                         client,
		collection:                     collection,
		indexName:                      indexName,
		searchAttributesProvider:       searchAttributesProvider,
		searchAttributesMapperProvider: searchAttributesMapperProvider,
		chasmRegistry:                  chasmRegistry,
		logger:                         logger,
	}
}

func (s *VisibilityStore) Close() {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.client.Close(ctx); err != nil {
		s.logger.Error("Failed to close MongoDB visibility client", tag.Error(err))
	}
}

func (s *VisibilityStore) GetName() string {
	return PersistenceName
}

func (s *VisibilityStore) GetIndexName() string {
	return s.indexName
}

func (s *VisibilityStore) ValidateCustomSearchAttributes(searchAttributes map[string]any) (map[string]any, error) {
	return searchAttributes, nil
}

func (s *VisibilityStore) RecordWorkflowExecutionStarted(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionStartedRequest,
) error {
	document, err := s.buildDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}
	return s.replaceVersioned(ctx, document)
}

func (s *VisibilityStore) RecordWorkflowExecutionClosed(
	ctx context.Context,
	request *store.InternalRecordWorkflowExecutionClosedRequest,
) error {
	document, err := s.buildClosedDocument(request)
	if err != nil {
		return err
	}
	return s.replaceVersioned(ctx, document)
}

func (s *VisibilityStore) UpsertWorkflowExecution(
	ctx context.Context,
	request *store.InternalUpsertWorkflowExecutionRequest,
) error {
	document, err := s.buildDocument(request.InternalVisibilityRequestBase)
	if err != nil {
		return err
	}
	return s.replaceVersioned(ctx, document)
}

func (s *VisibilityStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *manager.VisibilityDeleteWorkflowExecutionRequest,
) error {
	return s.replaceVersioned(ctx, tombstoneDocument(request))
}

func (s *VisibilityStore) replaceVersioned(ctx context.Context, document bson.M) error {
	id := document[documentIDField]
	version := document[versionField]
	filter := bson.D{
		{Key: documentIDField, Value: id},
		{Key: sadefs.NamespaceID, Value: document[sadefs.NamespaceID]},
		{Key: sadefs.RunID, Value: document[sadefs.RunID]},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: versionField, Value: bson.D{{Key: "$lt", Value: version}}}},
			bson.D{{Key: versionField, Value: bson.D{{Key: "$exists", Value: false}}}},
		}},
	}
	_, err := s.collection.UpdateOne(
		ctx,
		filter,
		mongo.Pipeline{{{Key: "$replaceWith", Value: document}}},
		options.Update().SetUpsert(true),
	)
	if err == nil {
		return nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return convertMongoError("write visibility document", err)
	}
	var current struct {
		Version int64 `bson:"_version"`
	}
	if findErr := s.collection.FindOne(ctx, bson.D{{Key: documentIDField, Value: id}}).Decode(&current); findErr != nil {
		return convertMongoError("verify visibility document version after duplicate key", findErr)
	}
	incomingVersion, ok := version.(int64)
	if !ok {
		return serviceerror.NewInternalf("visibility document version has type %T", version)
	}
	if current.Version >= incomingVersion {
		return nil
	}
	return serviceerror.NewUnavailablef(
		"visibility document write conflicted at version %d while stored version is %d",
		incomingVersion,
		current.Version,
	)
}

func convertMongoError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return serviceerror.NewUnavailablef("%s: %v", operation, err)
}

func (s *VisibilityStore) AddSearchAttributes(
	context.Context,
	*manager.AddSearchAttributesRequest,
) error {
	return nil
}

func (s *VisibilityStore) ensureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: sadefs.NamespaceID, Value: 1},
				{Key: deletedField, Value: 1},
				{Key: closeTimeSortField, Value: -1},
				{Key: sadefs.StartTime, Value: -1},
				{Key: sadefs.RunID, Value: -1},
			},
			Options: options.Index().SetName("visibility_default"),
		},
		{
			Keys: bson.D{
				{Key: sadefs.NamespaceID, Value: 1},
				{Key: deletedField, Value: 1},
				{Key: sadefs.WorkflowID, Value: 1},
				{Key: closeTimeSortField, Value: -1},
				{Key: sadefs.StartTime, Value: -1},
				{Key: sadefs.RunID, Value: -1},
			},
			Options: options.Index().SetName("visibility_workflow_id"),
		},
		{
			Keys: bson.D{
				{Key: sadefs.NamespaceID, Value: 1},
				{Key: deletedField, Value: 1},
				{Key: "$**", Value: 1},
			},
			Options: options.Index().
				SetName("visibility_search_attributes").
				SetWildcardProjection(bson.D{
					{Key: sadefs.NamespaceID, Value: 0},
					{Key: deletedField, Value: 0},
					{Key: documentIDField, Value: 0},
				}),
		},
	}
	for _, index := range indexes {
		if _, err := s.collection.Indexes().CreateOne(ctx, index); err != nil {
			return convertMongoError("create MongoDB visibility index", err)
		}
	}
	return nil
}
