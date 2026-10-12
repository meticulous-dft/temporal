package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

const (
	collectionShards = "shards"
)

type (
	shardStore struct {
		transactionalStore
		db                  client.Database
		cfg                 config.MongoDB
		logger              log.Logger
		clusterName         string
		transactionsEnabled bool
	}

	shardDocument struct {
		ShardID             int32  `bson:"_id"`
		RangeID             int64  `bson:"range_id"`
		Data                []byte `bson:"data"`
		DataEncoding        string `bson:"data_encoding"`
		TransactionLockID   string `bson:"transaction_lock_id,omitempty"`
		StorageBucketCount  int32  `bson:"storage_bucket_count"`
		StorageModelVersion string `bson:"storage_model_version"`
	}
)

// NewShardStore creates a new instance of MongoDB-based ShardStore
func NewShardStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	clusterName string,
	transactionsEnabled bool,
) (persistence.ShardStore, error) {
	if !transactionsEnabled {
		return nil, errors.New("mongodb shard store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb shard store requires client with session support")
	}
	return &shardStore{
		transactionalStore:  newTransactionalStore(mongoClient),
		db:                  db,
		cfg:                 cfg,
		logger:              logger,
		clusterName:         clusterName,
		transactionsEnabled: transactionsEnabled,
	}, nil
}

func (s *shardStore) GetName() string {
	return "mongodb"
}

func (s *shardStore) GetClusterName() string {
	return s.clusterName
}

func (s *shardStore) Close() {
	// Database connection is managed by Factory
}

func (s *shardStore) GetOrCreateShard(
	ctx context.Context,
	request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
	collection := s.db.Collection(collectionShards)

	filter := bson.M{"_id": request.ShardID}

	var doc shardDocument
	err := collection.FindOne(ctx, filter).Decode(&doc)

	if err == nil {
		_, err = s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
			if err := collection.FindOne(txCtx, filter).Decode(&doc); err != nil {
				return nil, newMongoUnavailableErrorf(err, "failed to read shard owner: %v", err)
			}
			return nil, validateBucketFences(txCtx, s.db, doc)
		})
		if err != nil {
			return nil, err
		}
		// Shard exists, return it
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: persistence.NewDataBlob(doc.Data, doc.DataEncoding),
		}, nil
	}

	if err != mongo.ErrNoDocuments {
		// Unexpected error
		return nil, serviceerror.NewUnavailablef(
			"GetOrCreateShard: failed to get ShardID %v. Error: %v",
			request.ShardID,
			err,
		)
	}

	// Shard doesn't exist
	if request.CreateShardInfo == nil {
		return nil, serviceerror.NewNotFoundf(
			"GetOrCreateShard: ShardID %v not found",
			request.ShardID,
		)
	}

	// Create new shard
	rangeID, shardInfo, err := request.CreateShardInfo()
	if err != nil {
		return nil, serviceerror.NewUnavailablef(
			"GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v",
			request.ShardID,
			err,
		)
	}

	newDoc := shardDocument{
		ShardID:             request.ShardID,
		RangeID:             rangeID,
		Data:                shardInfo.Data,
		DataEncoding:        shardInfo.EncodingType.String(),
		StorageBucketCount:  mongodbschema.StorageBucketCount,
		StorageModelVersion: mongodbschema.StorageModelVersion,
	}

	_, err = s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		if _, err := collection.InsertOne(txCtx, newDoc); err != nil {
			return nil, err
		}
		_, err := s.db.Collection(collectionHistoryRecords).InsertMany(txCtx, initialBucketFences(request.ShardID, rangeID))
		return nil, err
	})
	if err == nil {
		s.logger.Info("Created new shard",
			tag.ShardID(request.ShardID),
			tag.NewInt64("range-id", rangeID),
		)
		return &persistence.InternalGetOrCreateShardResponse{
			ShardInfo: shardInfo,
		}, nil
	}

	// Check for duplicate key error
	if mongo.IsDuplicateKeyError(err) {
		// Race condition: another process created the shard
		// Retry without creating
		readRequest := *request
		readRequest.CreateShardInfo = nil
		return s.GetOrCreateShard(ctx, &readRequest)
	}

	return nil, serviceerror.NewUnavailablef(
		"GetOrCreateShard: failed to insert shard %v. Error: %v",
		request.ShardID,
		err,
	)
}

func (s *shardStore) UpdateShard(
	ctx context.Context,
	request *persistence.InternalUpdateShardRequest,
) error {
	collection := s.db.Collection(collectionShards)

	// Optimistic concurrency control: update only if PreviousRangeID matches
	filter := bson.M{
		"_id":                   request.ShardID,
		"range_id":              request.PreviousRangeID,
		"storage_bucket_count":  mongodbschema.StorageBucketCount,
		"storage_model_version": mongodbschema.StorageModelVersion,
	}

	update := bson.M{
		"$set": bson.M{
			"range_id":      request.RangeID,
			"data":          request.ShardInfo.Data,
			"data_encoding": request.ShardInfo.EncodingType.String(),
		},
	}

	_, err := s.executeTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		fences, err := s.db.Collection(collectionHistoryRecords).UpdateMany(txCtx,
			bucketFencesFilter(request.ShardID, request.PreviousRangeID),
			bson.M{"$set": bson.M{"range_id": request.RangeID}},
		)
		if err != nil {
			return nil, newMongoUnavailableErrorf(err, "failed to update bucket fences: %v", err)
		}
		if fences.MatchedCount != mongodbschema.StorageBucketCount {
			return nil, &persistence.ShardOwnershipLostError{ShardID: request.ShardID, Msg: "incomplete or stale bucket fences"}
		}
		result, err := collection.UpdateOne(txCtx, filter, update)
		if err != nil {
			return nil, newMongoUnavailableErrorf(err, "failed to update shard owner: %v", err)
		}
		if result.MatchedCount != 1 {
			return nil, &persistence.ShardOwnershipLostError{ShardID: request.ShardID, Msg: "previous range ID or storage model mismatch"}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	s.logger.Debug("Updated shard",
		tag.ShardID(request.ShardID),
		tag.NewInt64("new-range-id", request.RangeID),
		tag.NewInt64("previous-range-id", request.PreviousRangeID),
	)

	return nil
}

func (s *shardStore) AssertShardOwnership(
	ctx context.Context,
	request *persistence.AssertShardOwnershipRequest,
) error {
	// AssertShardOwnership is not implemented for MongoDB shard store
	// Similar to SQL and Cassandra implementations
	return nil
}

// createIndexes creates indexes for the shards collection
func (s *shardStore) createIndexes(ctx context.Context) error {
	// The _id field (ShardID) already has an implicit unique index
	// range_id is used for optimistic locking but doesn't need a separate index
	// since we always query by _id first

	s.logger.Info("Shard store indexes ready",
		tag.NewStringTag("collection", collectionShards),
	)

	return nil
}
