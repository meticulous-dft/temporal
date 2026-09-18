package mongodb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

const (
	defaultMongoReadPreference = "primary"
	defaultMongoWriteConcern   = "majority"
)

type (
	// Factory vends store objects backed by MongoDB
	Factory struct {
		cfg                 config.MongoDB
		client              client.Client
		logger              log.Logger
		clusterName         string
		metricsHandler      metrics.Handler
		database            client.Database
		topologyInfo        *client.TopologyInfo
		transactionsEnabled bool

		sync.RWMutex
		taskStore          p.TaskStore
		fairTaskStore      p.TaskStore
		shardStore         p.ShardStore
		metadataStore      p.MetadataStore
		executionStore     p.ExecutionStore
		queue              p.Queue
		queueV2            p.QueueV2
		clusterMDStore     p.ClusterMetadataStore
		nexusEndpointStore p.NexusEndpointStore
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// datastores backed by MongoDB
func NewFactory(
	cfg config.MongoDB,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*Factory, error) {
	factory := &Factory{
		cfg:            cfg,
		clusterName:    clusterName,
		logger:         logger,
		metricsHandler: metricsHandler,
	}

	// Initialize MongoDB client
	if err := factory.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize MongoDB factory: %w", err)
	}

	return factory, nil
}

// initialize establishes MongoDB connection and detects topology
func (f *Factory) initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOpts, sessionOpts, err := buildMongoOptions(f.cfg)
	if err != nil {
		return err
	}

	// Create client
	mongoClient, err := client.NewClient(ctx, clientOpts, sessionOpts)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %w", err)
	}
	initialized := false
	defer func() {
		if !initialized {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer closeCancel()
			_ = mongoClient.Close(closeCtx)
		}
	}()

	// Connect and verify
	if err := mongoClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Detect topology and transaction support
	topologyInfo, err := mongoClient.GetTopology(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect MongoDB topology: %w", err)
	}

	if err := validateMongoTopology(topologyInfo.Type); err != nil {
		return err
	}

	database := mongoClient.Database(f.cfg.DatabaseName)
	version, err := NewSchemaVersionReader(database).ReadSchemaVersionInfo(ctx, f.cfg.DatabaseName)
	if err != nil {
		return fmt.Errorf("failed to read MongoDB schema topology: %w", err)
	}
	if err := validateMongoSchemaTopology(topologyInfo.Type, version); err != nil {
		return err
	}

	f.client = mongoClient
	f.database = database
	f.topologyInfo = topologyInfo
	f.transactionsEnabled = true
	initialized = true
	metrics.PersistenceMongoMaxPoolSize.With(f.metricsHandler).Record(float64(f.cfg.MaxConns))
	metrics.PersistenceMongoSessionsInProgress.With(f.metricsHandler).Record(float64(f.client.NumberSessionsInProgress()))

	f.logger.Info("MongoDB connection established",
		tag.NewStringTag("topology", f.topologyInfo.Type.String()),
		tag.NewBoolTag("transactions-enabled", f.transactionsEnabled),
		tag.NewStringTag("database", f.cfg.DatabaseName),
		tag.NewInt("max-conns", f.cfg.MaxConns),
		tag.NewStringTag("read-preference", clientOpts.ReadPreference.Mode().String()),
		tag.NewStringTag("write-concern", fmt.Sprint(clientOpts.WriteConcern.W)),
	)

	return nil
}

func validateMongoTopology(topologyType client.TopologyType) error {
	if topologyType != client.TopologyReplicaSet && topologyType != client.TopologySharded {
		return fmt.Errorf("mongo topology %q is unsupported; Temporal requires a replica set or validated sharded cluster", topologyType.String())
	}
	return nil
}

func validateMongoSchemaTopology(topologyType client.TopologyType, version schemaVersionDocument) error {
	switch topologyType {
	case client.TopologyReplicaSet:
		if version.Topology != "" && version.Topology != mongodbschema.TopologyReplicaSet {
			return fmt.Errorf("MongoDB schema topology %q does not match replica set deployment", version.Topology)
		}
		return nil
	case client.TopologySharded:
		if version.Topology != mongodbschema.TopologySharded {
			return fmt.Errorf("MongoDB sharded deployment requires schema topology %q; found %q", mongodbschema.TopologySharded, version.Topology)
		}
		if version.ShardingPlanVersion != mongodbschema.ShardingPlanVersion {
			return fmt.Errorf("MongoDB sharded deployment requires sharding plan version %q; found %q", mongodbschema.ShardingPlanVersion, version.ShardingPlanVersion)
		}
		return nil
	default:
		return validateMongoTopology(topologyType)
	}
}

func buildMongoOptions(cfg config.MongoDB) (*options.ClientOptions, *options.SessionOptions, error) {
	readPreferenceName := strings.TrimSpace(cfg.ReadPreference)
	if readPreferenceName == "" {
		readPreferenceName = defaultMongoReadPreference
	}
	if !strings.EqualFold(readPreferenceName, defaultMongoReadPreference) {
		return nil, nil, fmt.Errorf("invalid MongoDB readPreference %q: expected primary", cfg.ReadPreference)
	}
	readPreference := readpref.Primary()

	writeConcern, err := parseMongoWriteConcern(cfg.WriteConcern)
	if err != nil {
		return nil, nil, err
	}

	clientOptions := options.Client()
	if cfg.URI != "" {
		clientOptions = clientOptions.ApplyURI(cfg.URI)
	}
	clientOptions.
		SetConnectTimeout(cfg.ConnectTimeout).
		SetServerSelectionTimeout(cfg.ConnectTimeout).
		SetHeartbeatInterval(10 * time.Second).
		SetRetryReads(true).
		SetRetryWrites(true).
		SetReadPreference(readPreference).
		SetWriteConcern(writeConcern)
	if len(cfg.Hosts) > 0 {
		clientOptions.SetHosts(cfg.Hosts)
	}
	if cfg.ReplicaSet != "" {
		clientOptions.SetReplicaSet(cfg.ReplicaSet)
	}
	if cfg.User != "" || cfg.Password != "" {
		clientOptions.SetAuth(options.Credential{
			Username:   cfg.User,
			Password:   cfg.Password,
			AuthSource: cfg.AuthSource,
		})
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := auth.NewTLSConfig(cfg.TLS)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}

	if cfg.MaxConns > 0 {
		clientOptions.SetMaxPoolSize(uint64(cfg.MaxConns))
		if cfg.MaxConnecting > 0 {
			maxConnecting := cfg.MaxConnecting
			if maxConnecting > cfg.MaxConns {
				maxConnecting = cfg.MaxConns
			}
			clientOptions.SetMaxConnecting(uint64(maxConnecting))
		} else {
			clientOptions.SetMaxConnecting(uint64(cfg.MaxConns))
		}
	}

	if cfg.MinConns > 0 {
		clientOptions.SetMinPoolSize(uint64(cfg.MinConns))
	}

	if cfg.ConnIdleTime > 0 {
		clientOptions.SetMaxConnIdleTime(cfg.ConnIdleTime)
	}
	sessionOptions := options.Session().
		SetDefaultReadConcern(readconcern.Snapshot()).
		SetDefaultReadPreference(readpref.Primary()).
		SetDefaultWriteConcern(writeConcern)

	return clientOptions, sessionOptions, nil
}

// BuildMongoOptions creates the shared MongoDB client and session configuration.
func BuildMongoOptions(cfg config.MongoDB) (*options.ClientOptions, *options.SessionOptions, error) {
	return buildMongoOptions(cfg)
}

func parseMongoWriteConcern(value string) (*writeconcern.WriteConcern, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		normalized = defaultMongoWriteConcern
	}
	if normalized != defaultMongoWriteConcern {
		return nil, fmt.Errorf("invalid MongoDB writeConcern %q: expected majority", value)
	}
	return writeconcern.Majority(), nil
}

// Close closes the factory and underlying connections
func (f *Factory) Close() {
	if f.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.client.Close(ctx); err != nil {
			f.logger.Error("Failed to close MongoDB client", tag.Error(err))
		}
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.taskStore != nil {
		return f.taskStore, nil
	}

	taskStore, err := NewTaskStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.taskStore = taskStore
	return taskStore, nil
}

// NewFairTaskStore returns a new fair task store
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.fairTaskStore != nil {
		return f.fairTaskStore, nil
	}

	fairTaskStore, err := NewFairTaskStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.fairTaskStore = fairTaskStore
	return fairTaskStore, nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.shardStore != nil {
		return f.shardStore, nil
	}

	shardStore, err := NewShardStore(
		f.database,
		f.cfg,
		f.logger,
		f.clusterName,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.shardStore = shardStore
	return shardStore, nil
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.metadataStore != nil {
		return f.metadataStore, nil
	}

	metadataStore, err := NewMetadataStore(
		f.database,
		f.client,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.metadataStore = metadataStore
	return metadataStore, nil
}

// NewExecutionStore returns a new execution store
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.executionStore != nil {
		return f.executionStore, nil
	}

	executionStore, err := NewExecutionStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.executionStore = executionStore
	return executionStore, nil
}

// NewQueue returns a new queue backed by MongoDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	f.Lock()
	defer f.Unlock()

	if f.queue != nil {
		return f.queue, nil
	}

	queueStore, err := NewQueueStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
		queueType,
	)
	if err != nil {
		return nil, err
	}

	f.queue = queueStore
	return queueStore, nil
}

// NewQueueV2 returns a new QueueV2 backed by MongoDB
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	f.Lock()
	defer f.Unlock()

	if f.queueV2 != nil {
		return f.queueV2, nil
	}

	queueV2Store, err := NewQueueV2Store(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.queueV2 = queueV2Store
	return queueV2Store, nil
}

// NewClusterMetadataStore returns a new cluster metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.clusterMDStore != nil {
		return f.clusterMDStore, nil
	}

	clusterMetadataStore, err := NewClusterMetadataStore(f.database, f.logger)
	if err != nil {
		return nil, err
	}

	f.clusterMDStore = clusterMetadataStore
	return clusterMetadataStore, nil
}

// NewNexusEndpointStore returns a new nexus endpoint store
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.nexusEndpointStore != nil {
		return f.nexusEndpointStore, nil
	}

	endpointStore, err := NewNexusEndpointStore(
		f.database,
		f.client,
		f.metricsHandler,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.nexusEndpointStore = endpointStore
	return endpointStore, nil
}
