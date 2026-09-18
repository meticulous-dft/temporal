package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blang/semver/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.temporal.io/server/common/persistence/mongodb/client"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

type schemaVersionDocument struct {
	CurrentVersion       string `bson:"curr_version"`
	MinCompatibleVersion string `bson:"min_compatible_version"`
	Topology             string `bson:"topology,omitempty"`
	ShardingPlanVersion  string `bson:"sharding_plan_version,omitempty"`
}

type SchemaVersionReader struct {
	database client.Database
}

func NewSchemaVersionReader(database client.Database) *SchemaVersionReader {
	return &SchemaVersionReader{database: database}
}

func (r *SchemaVersionReader) ReadSchemaVersion(databaseName string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	version, err := r.ReadSchemaVersionInfo(ctx, databaseName)
	if err != nil {
		return "", err
	}
	return version.CurrentVersion, nil
}

func (r *SchemaVersionReader) ReadSchemaVersionInfo(
	ctx context.Context,
	databaseName string,
) (schemaVersionDocument, error) {
	var version schemaVersionDocument
	err := r.database.Collection(mongodbschema.SchemaVersionCollection).
		FindOne(ctx, bson.M{"_id": databaseName}).
		Decode(&version)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return schemaVersionDocument{}, fmt.Errorf("no MongoDB schema version found for database %q", databaseName)
		}
		return schemaVersionDocument{}, fmt.Errorf("unable to read MongoDB schema version for database %q: %w", databaseName, err)
	}
	if _, err := semver.ParseTolerant(version.CurrentVersion); err != nil {
		return schemaVersionDocument{}, fmt.Errorf("invalid MongoDB schema version %q for database %q: %w", version.CurrentVersion, databaseName, err)
	}
	if _, err := semver.ParseTolerant(version.MinCompatibleVersion); err != nil {
		return schemaVersionDocument{}, fmt.Errorf("invalid MongoDB minimum compatible schema version %q for database %q: %w", version.MinCompatibleVersion, databaseName, err)
	}
	return version, nil
}
