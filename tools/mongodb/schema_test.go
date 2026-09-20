package mongodb

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

func TestUpdaterAppliesMigrationsInVersionOrder(t *testing.T) {
	database := &fakeSchemaDatabase{version: VersionInfo{CurrentVersion: "0.0", MinCompatibleVersion: "0.0"}}
	updater := NewUpdater(database)

	err := updater.Run(context.Background(), testSchemaFS(), "versioned", "")
	require.NoError(t, err)
	require.Equal(t, []string{"create:one", "create:two"}, database.appliedCommands)
	require.Equal(t, []string{"0.0.0->1.0.0", "1.0.0->1.1.0"}, database.committedVersions)
	require.Equal(t, "1.1.0", database.version.CurrentVersion)
}

func TestUpdaterStopsAtTargetVersion(t *testing.T) {
	database := &fakeSchemaDatabase{version: VersionInfo{CurrentVersion: "0.0", MinCompatibleVersion: "0.0"}}

	err := NewUpdater(database).Run(context.Background(), testSchemaFS(), "versioned", "1.0")
	require.NoError(t, err)
	require.Equal(t, []string{"create:one"}, database.appliedCommands)
	require.Equal(t, []string{"0.0.0->1.0.0"}, database.committedVersions)
}

func TestUpdaterResumesFailedVersionWithoutReapplyingCommittedVersions(t *testing.T) {
	database := &fakeSchemaDatabase{
		version:         VersionInfo{CurrentVersion: "0.0", MinCompatibleVersion: "0.0"},
		failCommandOnce: "create:two",
	}
	updater := NewUpdater(database)

	err := updater.Run(context.Background(), testSchemaFS(), "versioned", "")
	require.ErrorContains(t, err, "injected command failure")
	require.Equal(t, "1.0.0", database.version.CurrentVersion)

	err = updater.Run(context.Background(), testSchemaFS(), "versioned", "")
	require.NoError(t, err)
	require.Equal(t, []string{"create:one", "create:two", "create:two"}, database.appliedCommands)
	require.Equal(t, []string{"0.0.0->1.0.0", "1.0.0->1.1.0"}, database.committedVersions)
}

func TestUpdaterRejectsMissingTargetVersion(t *testing.T) {
	database := &fakeSchemaDatabase{version: VersionInfo{CurrentVersion: "0.0", MinCompatibleVersion: "0.0"}}

	err := NewUpdater(database).Run(context.Background(), testSchemaFS(), "versioned", "1.2")
	require.ErrorContains(t, err, "target MongoDB schema version 1.2.0 is not present")
	require.Empty(t, database.appliedCommands)
	require.Equal(t, "0.0", database.version.CurrentVersion)
}

func TestUpdaterRejectsManifestDirectoryVersionMismatch(t *testing.T) {
	database := &fakeSchemaDatabase{version: VersionInfo{CurrentVersion: "0.0", MinCompatibleVersion: "0.0"}}
	schemaFS := fstest.MapFS{
		"versioned/v1.0/manifest.json": &fstest.MapFile{Data: []byte(`{
			"CurrVersion":"1.1",
			"MinCompatibleVersion":"1.0",
			"Description":"bad",
			"SchemaUpdateFiles":["schema.json"]
		}`)},
		"versioned/v1.0/schema.json": &fstest.MapFile{Data: []byte(`[{"create":"one"}]`)},
	}

	err := NewUpdater(database).Run(context.Background(), schemaFS, "versioned", "")
	require.ErrorContains(t, err, "does not match directory version")
}

func TestNewConnectionRejectsInvalidTLSConfiguration(t *testing.T) {
	_, err := NewConnection(context.Background(), config.MongoDB{
		Hosts:        []string{"localhost:27017"},
		DatabaseName: "temporal",
		TLS: &auth.TLS{
			Enabled:  true,
			CertFile: "client.pem",
		},
	})
	require.ErrorContains(t, err, "cert or key is missing")
}

func TestBuildClientOptionsAppliesURI(t *testing.T) {
	const uri = "mongodb://mongo.example.com:27017/?retryWrites=true"

	clientOptions, err := buildClientOptions(config.MongoDB{URI: uri})

	require.NoError(t, err)
	require.NoError(t, clientOptions.Validate())
	require.Equal(t, uri, clientOptions.GetURI())
}

func testSchemaFS() fstest.MapFS {
	return fstest.MapFS{
		"versioned/v1.1/manifest.json": &fstest.MapFile{Data: []byte(`{
			"CurrVersion":"1.1",
			"MinCompatibleVersion":"1.0",
			"Description":"second",
			"SchemaUpdateFiles":["schema.json"]
		}`)},
		"versioned/v1.1/schema.json": &fstest.MapFile{Data: []byte(`[{"create":"two"}]`)},
		"versioned/v1.0/manifest.json": &fstest.MapFile{Data: []byte(`{
			"CurrVersion":"1.0",
			"MinCompatibleVersion":"1.0",
			"Description":"first",
			"SchemaUpdateFiles":["schema.json"]
		}`)},
		"versioned/v1.0/schema.json": &fstest.MapFile{Data: []byte(`[{"create":"one"}]`)},
	}
}

type fakeSchemaDatabase struct {
	version           VersionInfo
	appliedCommands   []string
	committedVersions []string
	failCommandOnce   string
}

func (d *fakeSchemaDatabase) ReadSchemaVersion(context.Context) (VersionInfo, error) {
	return d.version, nil
}

func (d *fakeSchemaDatabase) ApplyCommand(_ context.Context, command bson.D) error {
	commandName := fmt.Sprintf("%s:%v", command[0].Key, command[0].Value)
	d.appliedCommands = append(d.appliedCommands, commandName)
	if d.failCommandOnce == commandName {
		d.failCommandOnce = ""
		return errors.New("injected command failure")
	}
	return nil
}

func (d *fakeSchemaDatabase) CommitVersion(_ context.Context, oldVersion string, update migration) error {
	if d.version.CurrentVersion != "0.0" && d.version.CurrentVersion != oldVersion {
		return fmt.Errorf("unexpected current version %s", d.version.CurrentVersion)
	}
	d.committedVersions = append(d.committedVersions, oldVersion+"->"+update.version.String())
	d.version = VersionInfo{
		CurrentVersion:       update.version.String(),
		MinCompatibleVersion: update.manifest.MinCompatibleVersion,
	}
	return nil
}
