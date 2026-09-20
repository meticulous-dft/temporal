package mongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

type (
	collectionSharding struct {
		Sharded  bool
		ShardKey bson.D
	}

	shardingDatabase interface {
		Topology(context.Context) (string, error)
		CollectionSharding(context.Context, string) (collectionSharding, error)
		CountCollection(context.Context, string) (int64, error)
		ShardCollection(context.Context, mongodbschema.ShardedCollection) error
		CommitTopology(context.Context, string, string) error
	}

	ShardingInstaller struct {
		database shardingDatabase
	}
)

func NewShardingInstaller(database shardingDatabase) *ShardingInstaller {
	return &ShardingInstaller{database: database}
}

func (i *ShardingInstaller) Run(ctx context.Context) error {
	topology, err := i.database.Topology(ctx)
	if err != nil {
		return err
	}
	if topology == mongodbschema.TopologyReplicaSet {
		return i.database.CommitTopology(ctx, topology, "")
	}
	if topology != mongodbschema.TopologySharded {
		return fmt.Errorf("unsupported MongoDB topology %q", topology)
	}

	for _, plan := range mongodbschema.ShardedCollections() {
		installed, err := i.database.CollectionSharding(ctx, plan.Name)
		if err != nil {
			return fmt.Errorf("inspect MongoDB sharding for collection %q: %w", plan.Name, err)
		}
		if installed.Sharded {
			if !sameShardKey(installed.ShardKey, plan.ShardKey) {
				return fmt.Errorf("MongoDB collection %q has shard key %v; expected %v", plan.Name, installed.ShardKey, plan.ShardKey)
			}
			continue
		}

		count, err := i.database.CountCollection(ctx, plan.Name)
		if err != nil {
			return fmt.Errorf("count MongoDB collection %q before sharding: %w", plan.Name, err)
		}
		if count != 0 {
			return fmt.Errorf("MongoDB collection %q contains %d documents and is not covered by the installed sharding plan", plan.Name, count)
		}
		if err := i.database.ShardCollection(ctx, plan); err != nil {
			return fmt.Errorf("shard MongoDB collection %q: %w", plan.Name, err)
		}
	}

	return i.database.CommitTopology(ctx, topology, mongodbschema.ShardingPlanVersion)
}

func sameShardKey(installed, expected bson.D) bool {
	if len(installed) != len(expected) {
		return false
	}
	for index := range installed {
		if installed[index].Key != expected[index].Key || !sameShardKeyValue(installed[index].Value, expected[index].Value) {
			return false
		}
	}
	return true
}

func sameShardKeyValue(installed interface{}, expected interface{}) bool {
	numericValue := func(value interface{}) (int64, bool) {
		switch direction := value.(type) {
		case int:
			return int64(direction), true
		case int32:
			return int64(direction), true
		case int64:
			return direction, true
		default:
			return 0, false
		}
	}
	installedNumber, installedIsNumber := numericValue(installed)
	expectedNumber, expectedIsNumber := numericValue(expected)
	if installedIsNumber || expectedIsNumber {
		return installedIsNumber && expectedIsNumber && installedNumber == expectedNumber
	}
	return reflect.DeepEqual(installed, expected)
}

func (c *Connection) Topology(ctx context.Context) (string, error) {
	var hello struct {
		Message string `bson:"msg"`
		SetName string `bson:"setName"`
	}
	if err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return "", fmt.Errorf("detect MongoDB topology: %w", err)
	}
	if hello.Message == "isdbgrid" {
		return mongodbschema.TopologySharded, nil
	}
	if hello.SetName != "" {
		return mongodbschema.TopologyReplicaSet, nil
	}
	return "", errors.New("MongoDB standalone topology is unsupported")
}

func (c *Connection) CollectionSharding(ctx context.Context, name string) (collectionSharding, error) {
	var metadata struct {
		Key     bson.D `bson:"key"`
		Dropped bool   `bson:"dropped"`
	}
	err := c.client.Database("config").Collection("collections").
		FindOne(ctx, bson.M{"_id": c.dbName + "." + name}).Decode(&metadata)
	if errors.Is(err, mongo.ErrNoDocuments) || metadata.Dropped {
		return collectionSharding{}, nil
	}
	if err != nil {
		return collectionSharding{}, err
	}
	return collectionSharding{Sharded: true, ShardKey: metadata.Key}, nil
}

func (c *Connection) CountCollection(ctx context.Context, name string) (int64, error) {
	return c.database.Collection(name).CountDocuments(ctx, bson.M{}, options.Count().SetLimit(1))
}

func (c *Connection) ShardCollection(ctx context.Context, plan mongodbschema.ShardedCollection) error {
	return c.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "shardCollection", Value: c.dbName + "." + plan.Name},
		{Key: "key", Value: plan.ShardKey},
	}).Err()
}

func (c *Connection) CommitTopology(ctx context.Context, topology, planVersion string) error {
	set := bson.M{
		"topology":   topology,
		"updated_at": time.Now().UTC(),
	}
	update := bson.M{"$set": set}
	if planVersion == "" {
		update["$unset"] = bson.M{"sharding_plan_version": ""}
	} else {
		set["sharding_plan_version"] = planVersion
	}
	result, err := c.database.Collection(mongodbschema.SchemaVersionCollection).UpdateOne(
		ctx,
		bson.M{"_id": c.dbName},
		update,
	)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("MongoDB schema version metadata for database %q was not found", c.dbName)
	}
	return nil
}
