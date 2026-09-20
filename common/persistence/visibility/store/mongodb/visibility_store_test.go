package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mongoclient "go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestReplaceVersionedUsesAtomicReplacementAndShardKey(t *testing.T) {
	collection := &versionedCollection{}
	visibilityStore := &VisibilityStore{collection: collection}
	document := bson.M{
		documentIDField:    "namespace\x00run",
		versionField:       int64(11),
		sadefs.NamespaceID: "namespace",
		sadefs.RunID:       "run",
	}

	err := visibilityStore.replaceVersioned(context.Background(), document)
	require.NoError(t, err)
	require.Equal(t, "namespace\x00run", bsonValue(t, collection.filter, documentIDField))
	require.Equal(t, "namespace", bsonValue(t, collection.filter, sadefs.NamespaceID))
	require.Equal(t, "run", bsonValue(t, collection.filter, sadefs.RunID))
	require.Equal(t, document, collection.replacement[0][0].Value)
	require.True(t, collection.options.Upsert != nil && *collection.options.Upsert)
}

func TestReplaceVersionedTreatsDuplicateFromNewerTaskAsSuccess(t *testing.T) {
	collection := &versionedCollection{
		updateErr: mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}},
		stored:    bson.M{versionField: int64(12)},
	}
	visibilityStore := &VisibilityStore{collection: collection}

	err := visibilityStore.replaceVersioned(context.Background(), bson.M{
		documentIDField:    "namespace\x00run",
		versionField:       int64(11),
		sadefs.NamespaceID: "namespace",
		sadefs.RunID:       "run",
	})
	require.NoError(t, err)
}

func TestReplaceVersionedRejectsUnexplainedDuplicate(t *testing.T) {
	collection := &versionedCollection{
		updateErr: mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}},
		stored:    bson.M{versionField: int64(10)},
	}
	visibilityStore := &VisibilityStore{collection: collection}

	err := visibilityStore.replaceVersioned(context.Background(), bson.M{
		documentIDField:    "namespace\x00run",
		versionField:       int64(11),
		sadefs.NamespaceID: "namespace",
		sadefs.RunID:       "run",
	})
	require.ErrorContains(t, err, "stored version is 10")
}

func TestReplaceVersionedPropagatesCancellation(t *testing.T) {
	collection := &versionedCollection{updateErr: context.Canceled}
	visibilityStore := &VisibilityStore{collection: collection}

	err := visibilityStore.replaceVersioned(context.Background(), bson.M{
		documentIDField:    "namespace\x00run",
		versionField:       int64(11),
		sadefs.NamespaceID: "namespace",
		sadefs.RunID:       "run",
	})
	require.ErrorIs(t, err, context.Canceled)
}

func bsonValue(t *testing.T, document bson.D, key string) any {
	t.Helper()
	for _, element := range document {
		if element.Key == key {
			return element.Value
		}
	}
	t.Fatalf("key %q not found in %v", key, document)
	return nil
}

type versionedCollection struct {
	mongoclient.Collection
	filter      bson.D
	replacement mongo.Pipeline
	options     *options.UpdateOptions
	updateErr   error
	stored      bson.M
}

func (c *versionedCollection) UpdateOne(
	_ context.Context,
	filter interface{},
	update interface{},
	updateOptions ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	c.filter = filter.(bson.D)
	c.replacement = update.(mongo.Pipeline)
	c.options = updateOptions[0]
	return &mongo.UpdateResult{}, c.updateErr
}

func (c *versionedCollection) FindOne(
	context.Context,
	interface{},
	...*options.FindOneOptions,
) mongoclient.SingleResult {
	return bsonSingleResult{value: c.stored}
}

type bsonSingleResult struct {
	value bson.M
	err   error
}

func (r bsonSingleResult) Decode(value interface{}) error {
	if r.err != nil {
		return r.err
	}
	data, err := bson.Marshal(r.value)
	if err != nil {
		return err
	}
	return bson.Unmarshal(data, value)
}

func (r bsonSingleResult) Err() error {
	return r.err
}
