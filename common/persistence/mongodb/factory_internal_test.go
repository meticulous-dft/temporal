package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

func TestBuildMongoOptions(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		clientOptions, sessionOptions, err := buildMongoOptions(config.MongoDB{})
		require.NoError(t, err)
		require.Equal(t, readpref.PrimaryMode, clientOptions.ReadPreference.Mode())
		require.Equal(t, "majority", clientOptions.WriteConcern.W)
		require.Equal(t, "snapshot", sessionOptions.DefaultReadConcern.Level)
		require.Equal(t, readpref.PrimaryMode, sessionOptions.DefaultReadPreference.Mode())
		require.Same(t, clientOptions.WriteConcern, sessionOptions.DefaultWriteConcern)
	})

	t.Run("configured", func(t *testing.T) {
		clientOptions, sessionOptions, err := buildMongoOptions(config.MongoDB{
			Hosts:          []string{"mongo-0:27017", "mongo-1:27017"},
			User:           "temporal@admin",
			Password:       "reserved:/?#[]@",
			AuthSource:     "admin",
			ReplicaSet:     "rs0",
			ReadPreference: "primary",
			WriteConcern:   "majority",
		})
		require.NoError(t, err)
		require.Equal(t, readpref.PrimaryMode, clientOptions.ReadPreference.Mode())
		require.Equal(t, "majority", clientOptions.WriteConcern.W)
		require.Equal(t, []string{"mongo-0:27017", "mongo-1:27017"}, clientOptions.Hosts)
		require.Equal(t, "rs0", *clientOptions.ReplicaSet)
		require.Equal(t, "temporal@admin", clientOptions.Auth.Username)
		require.Equal(t, "reserved:/?#[]@", clientOptions.Auth.Password)
		require.Equal(t, "admin", clientOptions.Auth.AuthSource)
		require.Equal(t, "snapshot", sessionOptions.DefaultReadConcern.Level)
		require.Equal(t, readpref.PrimaryMode, sessionOptions.DefaultReadPreference.Mode())
		require.Same(t, clientOptions.WriteConcern, sessionOptions.DefaultWriteConcern)
	})

	t.Run("URI", func(t *testing.T) {
		clientOptions, _, err := buildMongoOptions(config.MongoDB{
			URI:            "mongodb://mongo.example.com:27017/?retryWrites=false",
			User:           "temporal",
			Password:       "password",
			AuthSource:     "admin",
			ReadPreference: "primary",
			WriteConcern:   "majority",
		})
		require.NoError(t, err)
		require.Equal(t, "mongodb://mongo.example.com:27017/?retryWrites=false", clientOptions.GetURI())
		require.Equal(t, []string{"mongo.example.com:27017"}, clientOptions.Hosts)
		require.True(t, *clientOptions.RetryWrites)
		require.Equal(t, "temporal", clientOptions.Auth.Username)
	})
}

func TestValidateMongoTopology(t *testing.T) {
	tests := []struct {
		name         string
		topologyType client.TopologyType
		wantError    bool
	}{
		{name: "replica set", topologyType: client.TopologyReplicaSet},
		{name: "sharded", topologyType: client.TopologySharded},
		{name: "standalone", topologyType: client.TopologyStandalone, wantError: true},
		{name: "unknown", topologyType: client.TopologyUnknown, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMongoTopology(test.topologyType)
			if test.wantError {
				require.ErrorContains(t, err, "requires a replica set or validated sharded cluster")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateMongoSchemaTopology(t *testing.T) {
	tests := []struct {
		name         string
		topologyType client.TopologyType
		version      schemaVersionDocument
		wantError    string
	}{
		{name: "legacy replica set", topologyType: client.TopologyReplicaSet},
		{name: "stamped replica set", topologyType: client.TopologyReplicaSet, version: schemaVersionDocument{Topology: "replica_set"}},
		{name: "sharded exact plan", topologyType: client.TopologySharded, version: schemaVersionDocument{Topology: "sharded", ShardingPlanVersion: "2"}},
		{name: "sharded missing topology", topologyType: client.TopologySharded, wantError: `requires schema topology "sharded"`},
		{name: "sharded missing plan", topologyType: client.TopologySharded, version: schemaVersionDocument{Topology: "sharded"}, wantError: `requires sharding plan version "2"`},
		{name: "sharded wrong plan", topologyType: client.TopologySharded, version: schemaVersionDocument{Topology: "sharded", ShardingPlanVersion: "1"}, wantError: `requires sharding plan version "2"`},
		{name: "replica set with sharded metadata", topologyType: client.TopologyReplicaSet, version: schemaVersionDocument{Topology: "sharded", ShardingPlanVersion: "1"}, wantError: "does not match replica set deployment"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMongoSchemaTopology(test.topologyType, test.version)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
		})
	}
}

func TestBuildMongoOptionsRejectsInvalidConcerns(t *testing.T) {
	tests := []struct {
		name       string
		cfg        config.MongoDB
		wantErrMsg string
	}{
		{
			name:       "unknown read preference",
			cfg:        config.MongoDB{ReadPreference: "eventually"},
			wantErrMsg: `invalid MongoDB readPreference "eventually"`,
		},
		{
			name:       "secondary reads",
			cfg:        config.MongoDB{ReadPreference: "secondaryPreferred"},
			wantErrMsg: `invalid MongoDB readPreference "secondaryPreferred"`,
		},
		{
			name:       "primary preferred reads",
			cfg:        config.MongoDB{ReadPreference: "primaryPreferred"},
			wantErrMsg: `invalid MongoDB readPreference "primaryPreferred"`,
		},
		{
			name:       "secondary only reads",
			cfg:        config.MongoDB{ReadPreference: "secondary"},
			wantErrMsg: `invalid MongoDB readPreference "secondary"`,
		},
		{
			name:       "nearest reads",
			cfg:        config.MongoDB{ReadPreference: "nearest"},
			wantErrMsg: `invalid MongoDB readPreference "nearest"`,
		},
		{
			name:       "unacknowledged writes",
			cfg:        config.MongoDB{WriteConcern: "w0"},
			wantErrMsg: `invalid MongoDB writeConcern "w0"`,
		},
		{
			name:       "acknowledged but non-majority writes",
			cfg:        config.MongoDB{WriteConcern: "w2"},
			wantErrMsg: `invalid MongoDB writeConcern "w2"`,
		},
		{
			name:       "single acknowledged write",
			cfg:        config.MongoDB{WriteConcern: "w1"},
			wantErrMsg: `invalid MongoDB writeConcern "w1"`,
		},
		{
			name:       "three acknowledged writes",
			cfg:        config.MongoDB{WriteConcern: "w3"},
			wantErrMsg: `invalid MongoDB writeConcern "w3"`,
		},
		{
			name:       "malformed write concern",
			cfg:        config.MongoDB{WriteConcern: "two"},
			wantErrMsg: `invalid MongoDB writeConcern "two"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := buildMongoOptions(test.cfg)
			require.ErrorContains(t, err, test.wantErrMsg)
		})
	}
}
