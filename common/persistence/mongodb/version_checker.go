package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/blang/semver/v4"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/resolver"
	mongodbschema "go.temporal.io/server/schema/mongodb"
)

func VerifyCompatibleVersion(
	cfg config.Persistence,
	_ resolver.ServiceResolver,
	logger log.Logger,
) error {
	ds, ok := cfg.DataStores[cfg.DefaultStore]
	if !ok || ds.MongoDB == nil {
		return nil
	}
	return CheckCompatibleVersion(*ds.MongoDB, logger)
}

func CheckCompatibleVersion(cfg config.MongoDB, logger log.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientOptions, sessionOptions, err := buildMongoOptions(cfg)
	if err != nil {
		return err
	}
	mongoClient, err := client.NewClient(ctx, clientOptions, sessionOptions)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB schema version client: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if closeErr := mongoClient.Close(closeCtx); closeErr != nil {
			logger.Warn("failed to close MongoDB schema version client", tag.Error(closeErr))
		}
	}()

	if err := mongoClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect MongoDB schema version client: %w", err)
	}
	topology, err := mongoClient.GetTopology(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect MongoDB topology for schema compatibility: %w", err)
	}
	if err := validateMongoTopology(topology.Type); err != nil {
		return err
	}
	reader := NewSchemaVersionReader(mongoClient.Database(cfg.DatabaseName))
	version, err := reader.ReadSchemaVersionInfo(ctx, cfg.DatabaseName)
	if err != nil {
		return fmt.Errorf("MongoDB schema version compatibility check failed: %w", err)
	}
	currentVersion, _ := semver.ParseTolerant(version.CurrentVersion)
	minCompatibleVersion, _ := semver.ParseTolerant(version.MinCompatibleVersion)
	expectedVersion, _ := semver.ParseTolerant(mongodbschema.Version)
	if currentVersion.LT(expectedVersion) {
		return fmt.Errorf("MongoDB schema version compatibility check failed: database %q is at version %s; server requires at least %s", cfg.DatabaseName, currentVersion, expectedVersion)
	}
	if expectedVersion.LT(minCompatibleVersion) {
		return fmt.Errorf("MongoDB schema version compatibility check failed: database %q requires server schema compatibility %s; server supports %s", cfg.DatabaseName, minCompatibleVersion, expectedVersion)
	}
	return validateMongoSchemaTopology(topology.Type, version)
}
