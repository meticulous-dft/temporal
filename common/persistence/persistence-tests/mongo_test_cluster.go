package persistencetests

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/temporal/environment"
	mongodbschematool "go.temporal.io/server/tools/mongodb"
)

type MongoTestCluster struct {
	cfg            config.MongoDB
	logger         log.Logger
	faultInjection *config.FaultInjection
}

func NewMongoTestCluster(cfg config.MongoDB, logger log.Logger, faultInjection *config.FaultInjection) *MongoTestCluster {
	return &MongoTestCluster{
		cfg:            cfg,
		logger:         logger,
		faultInjection: faultInjection,
	}
}

func (c *MongoTestCluster) SetupTestDatabase() {
	if err := dropMongoDatabase(c.cfg); err != nil {
		c.logger.Error("failed to drop mongo database before setup", tag.Error(err))
	}
	if err := InitializeMongoSchema(c.cfg); err != nil {
		c.logger.Fatal("failed to initialize MongoDB schema", tag.Error(err))
	}
}

func InitializeMongoSchema(cfg config.MongoDB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return mongodbschematool.InstallDefaultSchema(ctx, cfg)
}

func (c *MongoTestCluster) TearDownTestDatabase() {
	if err := dropMongoDatabase(c.cfg); err != nil {
		c.logger.Error("failed to drop mongo database during teardown", tag.Error(err))
	}
}

func (c *MongoTestCluster) Config() config.Persistence {
	storeName := "test-mongodb"
	cfgCopy := c.cfg
	return config.Persistence{
		DefaultStore:    storeName,
		VisibilityStore: storeName,
		DataStores: map[string]config.DataStore{
			storeName: {
				MongoDB:        &cfgCopy,
				FaultInjection: c.faultInjection,
			},
		},
		TransactionSizeLimit: dynamicconfig.GetIntPropertyFn(primitives.DefaultTransactionSizeLimit),
	}
}

func NewTestBaseWithMongoDB(opts *TestBaseOptions) *TestBase {
	cfg := opts.MongoDBConfig
	if cfg == nil {
		defaults := GetMongoDBTestClusterOption()
		cfg = defaults.MongoDBConfig
	}
	cfgCopy := *cfg
	if opts.DBName != "" {
		cfgCopy.DatabaseName = opts.DBName
	}
	if cfgCopy.DatabaseName == "" {
		cfgCopy.DatabaseName = "test_" + GenerateRandomDBName(4) + "_temporal_persistence"
	}
	if len(cfgCopy.Hosts) == 0 {
		cfgCopy.Hosts = mongoDBTestHosts()
	}
	if cfgCopy.ConnectTimeout == 0 {
		cfgCopy.ConnectTimeout = 10 * time.Second
	}
	if cfgCopy.ReplicaSet == "" && os.Getenv("TEMPORAL_MONGODB_SHARDED_TEST") == "" {
		cfgCopy.ReplicaSet = environment.GetMongoDBReplicaSet()
	}
	if cfgCopy.ReadPreference == "" {
		cfgCopy.ReadPreference = "primary"
	}
	if cfgCopy.WriteConcern == "" {
		cfgCopy.WriteConcern = "majority"
	}
	if opts.Logger == nil {
		opts.Logger = log.NewTestLogger()
	}
	opts.MongoDBConfig = &cfgCopy
	cluster := NewMongoTestCluster(cfgCopy, opts.Logger, opts.FaultInjection)
	return NewTestBaseForCluster(cluster, opts.Logger)
}

func GetMongoDBTestClusterOption() *TestBaseOptions {
	cfg := &config.MongoDB{
		User:           "temporal",
		Password:       "temporal",
		AuthSource:     "admin",
		Hosts:          mongoDBTestHosts(),
		DatabaseName:   "test_" + GenerateRandomDBName(4) + "_temporal_persistence",
		MaxConns:       200,
		MinConns:       20,
		ConnectTimeout: 10 * time.Second,
		ReplicaSet:     mongoDBTestReplicaSet(),
		ReadPreference: "primary",
		WriteConcern:   "majority",
	}
	return &TestBaseOptions{
		StoreType:         config.StoreTypeNoSQL,
		NoSQLDBPluginName: "mongodb",
		DBName:            cfg.DatabaseName,
		MongoDBConfig:     cfg,
	}
}

func mongoDBTestReplicaSet() string {
	if os.Getenv("TEMPORAL_MONGODB_SHARDED_TEST") != "" {
		return ""
	}
	return environment.GetMongoDBReplicaSet()
}

func mongoDBTestHosts() []string {
	if seeds := os.Getenv("MONGODB_SEEDS"); seeds != "" {
		var hosts []string
		for _, seed := range strings.Split(seeds, ",") {
			if seed = strings.TrimSpace(seed); seed != "" {
				hosts = append(hosts, seed)
			}
		}
		if len(hosts) > 0 {
			return hosts
		}
	}
	return []string{net.JoinHostPort(environment.GetMongoDBAddress(), strconv.Itoa(environment.GetMongoDBPort()))}
}

func dropMongoDatabase(cfg config.MongoDB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(buildMongoURI(cfg, "")))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	dropErr := client.Database(cfg.DatabaseName).Drop(ctx)
	disconnectErr := client.Disconnect(ctx)
	if dropErr != nil {
		return fmt.Errorf("drop mongo database %q: %w", cfg.DatabaseName, dropErr)
	}
	if disconnectErr != nil {
		return fmt.Errorf("disconnect mongo client: %w", disconnectErr)
	}
	return nil
}

func buildMongoURI(cfg config.MongoDB, database string) string {
	uri := "mongodb://"
	if cfg.User != "" && cfg.Password != "" {
		uri += fmt.Sprintf("%s:%s@", cfg.User, cfg.Password)
	}
	uri += strings.Join(cfg.Hosts, ",")
	uri += "/" + database

	var opts []string
	if cfg.ReplicaSet != "" {
		opts = append(opts, fmt.Sprintf("replicaSet=%s", cfg.ReplicaSet))
	}
	if cfg.AuthSource != "" {
		opts = append(opts, fmt.Sprintf("authSource=%s", cfg.AuthSource))
	}
	if len(opts) > 0 {
		uri += "?" + strings.Join(opts, "&")
	}
	return uri
}
