package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMongoDBPOCConfiguration(t *testing.T) {
	t.Setenv("MONGODB_URI", "mongodb+srv://cluster.example.com")
	t.Setenv("MONGODB_DATABASE", "fresh_customer_poc")
	t.Setenv("MONGODB_USER", "poc")
	t.Setenv("MONGODB_PASSWORD", "quote\"colon:backslash\\")
	t.Setenv("MONGODB_AUTH_SOURCE", "")
	t.Setenv("MONGODB_TLS", "")
	t.Setenv("NUM_HISTORY_SHARDS", "")
	t.Setenv("BIND_ON_IP", "0.0.0.0")
	t.Setenv("TEMPORAL_BROADCAST_ADDRESS", "192.0.2.10")

	cfg, err := Load(WithConfigFile("../../config/mongodb-poc.yaml"))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	require.Equal(t, int32(32), cfg.Persistence.NumHistoryShards)
	require.Equal(t, "mongodb", cfg.Persistence.DefaultStore)
	require.Equal(t, "mongodb", cfg.Persistence.VisibilityStore)
	database := cfg.Persistence.DataStores["mongodb"].MongoDB
	require.Equal(t, "mongodb+srv://cluster.example.com", database.URI)
	require.Empty(t, database.Hosts)
	require.Empty(t, database.ReplicaSet)
	require.Equal(t, "fresh_customer_poc", database.DatabaseName)
	require.Equal(t, "quote\"colon:backslash\\", database.Password)
	require.Equal(t, "primary", database.ReadPreference)
	require.Equal(t, "majority", database.WriteConcern)
	require.True(t, database.TLS.Enabled)
	require.True(t, database.TLS.EnableHostVerification)
	require.Equal(t, "0.0.0.0", cfg.Services["frontend"].RPC.BindOnIP)

	t.Setenv("MONGODB_URI", "")
	cfg, err = Load(WithConfigFile("../../config/mongodb-poc.yaml"))
	require.NoError(t, err)
	require.ErrorContains(t, cfg.Validate(), "exactly one of uri or hosts")
}
