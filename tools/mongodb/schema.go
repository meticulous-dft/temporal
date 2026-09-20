package mongodb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	dbschemas "go.temporal.io/server/schema"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

var ErrNoSchemaVersion = errors.New("MongoDB schema version is not initialized")

type (
	VersionInfo struct {
		CurrentVersion       string `bson:"curr_version"`
		MinCompatibleVersion string `bson:"min_compatible_version"`
		Topology             string `bson:"topology,omitempty"`
		ShardingPlanVersion  string `bson:"sharding_plan_version,omitempty"`
	}

	manifest struct {
		CurrentVersion       string   `json:"CurrVersion"`
		MinCompatibleVersion string   `json:"MinCompatibleVersion"`
		Description          string   `json:"Description"`
		SchemaUpdateFiles    []string `json:"SchemaUpdateFiles"`
	}

	migration struct {
		version     semver.Version
		manifest    manifest
		commands    []bson.D
		manifestSHA string
	}

	schemaDatabase interface {
		ReadSchemaVersion(context.Context) (VersionInfo, error)
		ApplyCommand(context.Context, bson.D) error
		CommitVersion(context.Context, string, migration) error
	}

	Updater struct {
		database schemaDatabase
	}

	Connection struct {
		client   *mongo.Client
		database *mongo.Database
		dbName   string
	}
)

func NewUpdater(database schemaDatabase) *Updater {
	return &Updater{database: database}
}

func InstallDefaultSchema(ctx context.Context, cfg config.MongoDB) error {
	connection, err := NewConnection(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close(context.Background()) }()

	if err := connection.SetupSchema(ctx, "0.0"); err != nil {
		return err
	}
	if err := NewUpdater(connection).Run(ctx, dbschemas.Assets(), "mongodb/temporal/versioned", ""); err != nil {
		return err
	}
	return NewShardingInstaller(connection).Run(ctx)
}

func (u *Updater) Run(ctx context.Context, schemaFS fs.FS, versionedDir, targetVersion string) error {
	versionInfo, err := u.database.ReadSchemaVersion(ctx)
	if err != nil {
		return err
	}
	currentVersion, err := semver.ParseTolerant(versionInfo.CurrentVersion)
	if err != nil {
		return fmt.Errorf("invalid installed MongoDB schema version %q: %w", versionInfo.CurrentVersion, err)
	}

	target := semver.Version{}
	if targetVersion != "" {
		target, err = semver.ParseTolerant(targetVersion)
		if err != nil {
			return fmt.Errorf("invalid target MongoDB schema version %q: %w", targetVersion, err)
		}
		if target.LT(currentVersion) {
			return fmt.Errorf("target MongoDB schema version %s is older than installed version %s", target, currentVersion)
		}
	}

	migrations, err := loadMigrations(schemaFS, versionedDir, currentVersion, target)
	if err != nil {
		return err
	}
	if targetVersion != "" && !currentVersion.EQ(target) &&
		(len(migrations) == 0 || !migrations[len(migrations)-1].version.EQ(target)) {
		return fmt.Errorf("target MongoDB schema version %s is not present in %q", target, versionedDir)
	}
	for _, update := range migrations {
		for _, command := range update.commands {
			if err := u.database.ApplyCommand(ctx, command); err != nil {
				return fmt.Errorf("apply MongoDB schema version %s: %w", update.version, err)
			}
		}
		if err := u.database.CommitVersion(ctx, currentVersion.String(), update); err != nil {
			return fmt.Errorf("commit MongoDB schema version %s: %w", update.version, err)
		}
		currentVersion = update.version
	}

	return nil
}

func loadMigrations(schemaFS fs.FS, versionedDir string, currentVersion, targetVersion semver.Version) ([]migration, error) {
	entries, err := fs.ReadDir(schemaFS, versionedDir)
	if err != nil {
		return nil, fmt.Errorf("read MongoDB schema directory %q: %w", versionedDir, err)
	}

	var migrations []migration
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "v") {
			continue
		}
		version, err := semver.ParseTolerant(strings.TrimPrefix(entry.Name(), "v"))
		if err != nil {
			return nil, fmt.Errorf("invalid MongoDB schema directory %q: %w", entry.Name(), err)
		}
		if !version.GT(currentVersion) || (targetVersion.NE(semver.Version{}) && version.GT(targetVersion)) {
			continue
		}
		update, err := loadMigration(schemaFS, path.Join(versionedDir, entry.Name()), version)
		if err != nil {
			return nil, err
		}
		migrations = append(migrations, update)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version.LT(migrations[j].version) })
	return migrations, nil
}

func loadMigration(schemaFS fs.FS, migrationDir string, version semver.Version) (migration, error) {
	manifestBytes, err := fs.ReadFile(schemaFS, path.Join(migrationDir, "manifest.json"))
	if err != nil {
		return migration{}, fmt.Errorf("read MongoDB schema manifest for %s: %w", version, err)
	}
	var schemaManifest manifest
	if err := json.Unmarshal(manifestBytes, &schemaManifest); err != nil {
		return migration{}, fmt.Errorf("decode MongoDB schema manifest for %s: %w", version, err)
	}
	manifestVersion, err := semver.ParseTolerant(schemaManifest.CurrentVersion)
	if err != nil || !manifestVersion.EQ(version) {
		return migration{}, fmt.Errorf("MongoDB schema manifest version %q does not match directory version %s", schemaManifest.CurrentVersion, version)
	}
	if _, err := semver.ParseTolerant(schemaManifest.MinCompatibleVersion); err != nil {
		return migration{}, fmt.Errorf("invalid minimum compatible version %q for MongoDB schema %s: %w", schemaManifest.MinCompatibleVersion, version, err)
	}
	if len(schemaManifest.SchemaUpdateFiles) == 0 {
		return migration{}, fmt.Errorf("MongoDB schema manifest for %s contains no update files", version)
	}

	hash := sha256.New()
	_, _ = hash.Write(manifestBytes)
	var commands []bson.D
	for _, fileName := range schemaManifest.SchemaUpdateFiles {
		commandBytes, err := fs.ReadFile(schemaFS, path.Join(migrationDir, fileName))
		if err != nil {
			return migration{}, fmt.Errorf("read MongoDB schema command file %q: %w", fileName, err)
		}
		_, _ = hash.Write(commandBytes)
		var rawCommands []json.RawMessage
		if err := json.Unmarshal(commandBytes, &rawCommands); err != nil {
			return migration{}, fmt.Errorf("decode MongoDB schema command file %q: %w", fileName, err)
		}
		for commandIndex, rawCommand := range rawCommands {
			var command bson.D
			if err := bson.UnmarshalExtJSON(rawCommand, false, &command); err != nil {
				return migration{}, fmt.Errorf("decode command %d in %q: %w", commandIndex, fileName, err)
			}
			if len(command) == 0 {
				return migration{}, fmt.Errorf("command %d in %q is empty", commandIndex, fileName)
			}
			commands = append(commands, command)
		}
	}

	return migration{
		version:     version,
		manifest:    schemaManifest,
		commands:    commands,
		manifestSHA: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

func NewConnection(ctx context.Context, cfg config.MongoDB) (*Connection, error) {
	clientOptions, err := buildClientOptions(cfg)
	if err != nil {
		return nil, err
	}

	mongoClient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connect MongoDB schema tool: %w", err)
	}
	if err := mongoClient.Ping(ctx, readpref.Primary()); err != nil {
		_ = mongoClient.Disconnect(ctx)
		return nil, fmt.Errorf("ping MongoDB schema tool: %w", err)
	}
	return &Connection{
		client:   mongoClient,
		database: mongoClient.Database(cfg.DatabaseName),
		dbName:   cfg.DatabaseName,
	}, nil
}

func buildClientOptions(cfg config.MongoDB) (*options.ClientOptions, error) {
	clientOptions := options.Client()
	if cfg.URI != "" {
		clientOptions.ApplyURI(cfg.URI)
	}
	clientOptions.
		SetReadPreference(readpref.Primary()).
		SetWriteConcern(writeconcern.Majority()).
		SetRetryReads(true).
		SetRetryWrites(true)
	if len(cfg.Hosts) > 0 {
		clientOptions.SetHosts(cfg.Hosts)
	}
	if cfg.ConnectTimeout > 0 {
		clientOptions.SetConnectTimeout(cfg.ConnectTimeout).SetServerSelectionTimeout(cfg.ConnectTimeout)
	}
	if cfg.ReplicaSet != "" {
		clientOptions.SetReplicaSet(cfg.ReplicaSet)
	}
	if cfg.User != "" || cfg.Password != "" {
		clientOptions.SetAuth(options.Credential{
			AuthSource: cfg.AuthSource,
			Username:   cfg.User,
			Password:   cfg.Password,
		})
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		tlsConfig, err := auth.NewTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("create MongoDB schema tool TLS configuration: %w", err)
		}
		clientOptions.SetTLSConfig(tlsConfig)
	}
	if err := clientOptions.Validate(); err != nil {
		return nil, fmt.Errorf("validate MongoDB schema tool client options: %w", err)
	}
	return clientOptions, nil
}

func (c *Connection) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *Connection) SetupSchema(ctx context.Context, initialVersion string) error {
	version, err := semver.ParseTolerant(initialVersion)
	if err != nil {
		return fmt.Errorf("invalid initial MongoDB schema version %q: %w", initialVersion, err)
	}
	for _, collection := range []string{
		mongodbschema.SchemaVersionCollection,
		mongodbschema.SchemaUpdateHistoryCollection,
	} {
		err := c.database.RunCommand(ctx, bson.D{{Key: "create", Value: collection}}).Err()
		var commandError mongo.CommandError
		if err != nil && (!errors.As(err, &commandError) || commandError.Code != 48) {
			return fmt.Errorf("create MongoDB schema collection %q: %w", collection, err)
		}
	}

	_, err = c.database.Collection(mongodbschema.SchemaVersionCollection).InsertOne(ctx, bson.M{
		"_id":                    c.dbName,
		"curr_version":           version.String(),
		"min_compatible_version": version.String(),
		"updated_at":             time.Now().UTC(),
	})
	if err == nil {
		return nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("initialize MongoDB schema version: %w", err)
	}
	existing, readErr := c.ReadSchemaVersion(ctx)
	if readErr != nil {
		return readErr
	}
	if existing.CurrentVersion != version.String() || existing.MinCompatibleVersion != version.String() {
		return fmt.Errorf("MongoDB schema is already initialized at version %s with minimum compatible version %s", existing.CurrentVersion, existing.MinCompatibleVersion)
	}
	return nil
}

func (c *Connection) ReadSchemaVersion(ctx context.Context) (VersionInfo, error) {
	var version VersionInfo
	err := c.database.Collection(mongodbschema.SchemaVersionCollection).
		FindOne(ctx, bson.M{"_id": c.dbName}).Decode(&version)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return VersionInfo{}, ErrNoSchemaVersion
	}
	if err != nil {
		return VersionInfo{}, fmt.Errorf("read MongoDB schema version: %w", err)
	}
	if version.CurrentVersion == "" || version.MinCompatibleVersion == "" {
		return VersionInfo{}, errors.New("MongoDB schema version metadata is incomplete")
	}
	return version, nil
}

func (c *Connection) ApplyCommand(ctx context.Context, command bson.D) error {
	err := c.database.RunCommand(ctx, command).Err()
	var commandError mongo.CommandError
	if err != nil && errors.As(err, &commandError) && commandError.Code == 48 {
		return nil
	}
	return err
}

func (c *Connection) CommitVersion(ctx context.Context, oldVersion string, update migration) error {
	session, err := c.client.StartSession(options.Session().
		SetDefaultReadConcern(readconcern.Snapshot()).
		SetDefaultReadPreference(readpref.Primary()).
		SetDefaultWriteConcern(writeconcern.Majority()))
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(transactionContext mongo.SessionContext) (interface{}, error) {
		result, err := c.database.Collection(mongodbschema.SchemaVersionCollection).UpdateOne(
			transactionContext,
			bson.M{"_id": c.dbName, "curr_version": oldVersion},
			bson.M{"$set": bson.M{
				"curr_version":           update.version.String(),
				"min_compatible_version": update.manifest.MinCompatibleVersion,
				"updated_at":             time.Now().UTC(),
			}},
		)
		if err != nil {
			return nil, err
		}
		if result.MatchedCount != 1 {
			return nil, fmt.Errorf("installed MongoDB schema version changed from expected version %s", oldVersion)
		}
		_, err = c.database.Collection(mongodbschema.SchemaUpdateHistoryCollection).InsertOne(transactionContext, bson.M{
			"_id":             update.version.String(),
			"old_version":     oldVersion,
			"new_version":     update.version.String(),
			"manifest_sha256": update.manifestSHA,
			"description":     update.manifest.Description,
			"update_time":     time.Now().UTC(),
		})
		return nil, err
	})
	return err
}
