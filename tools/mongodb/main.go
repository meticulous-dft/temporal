package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	dbschemas "go.temporal.io/server/schema"
	"go.temporal.io/server/temporal/environment"
	commonschema "go.temporal.io/server/tools/common/schema"
)

const (
	flagURI          = "uri"
	flagEndpoint     = "endpoint"
	flagUser         = "user"
	flagPassword     = "password"
	flagDatabase     = "database"
	flagAuthSource   = "auth-source"
	flagReplicaSet   = "replica-set"
	flagVersion      = "version"
	flagSchemaDir    = "schema-dir"
	flagSchemaName   = "schema-name"
	defaultSchemaDir = "mongodb/temporal"
)

func RunTool(args []string) error {
	return BuildCLIOptions().Run(args)
}

func BuildCLIOptions() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal-mongodb-tool"
	app.Usage = "Command line tool for Temporal MongoDB schema operations"
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: flagURI, Usage: "MongoDB connection string", EnvVar: "MONGODB_URI"},
		cli.StringFlag{Name: flagEndpoint + ", ep", Value: environment.GetMongoDBAddress() + ":" + fmt.Sprint(environment.GetMongoDBPort()), EnvVar: "MONGODB_SEEDS"},
		cli.StringFlag{Name: flagUser + ", u", EnvVar: "MONGODB_USER"},
		cli.StringFlag{Name: flagPassword + ", pw", EnvVar: "MONGODB_PASSWORD"},
		cli.StringFlag{Name: flagDatabase + ", db", Value: "temporal", EnvVar: "MONGODB_DATABASE"},
		cli.StringFlag{Name: flagAuthSource, Value: "admin", EnvVar: "MONGODB_AUTH_SOURCE"},
		cli.StringFlag{Name: flagReplicaSet, Value: environment.GetMongoDBReplicaSet(), EnvVar: "MONGODB_REPLICA_SET"},
		cli.BoolFlag{Name: commonschema.CLIFlagEnableTLS, Usage: "enable TLS over the MongoDB connection", EnvVar: "MONGODB_TLS"},
		cli.StringFlag{Name: commonschema.CLIFlagTLSCertFile, Usage: "MongoDB TLS client certificate path", EnvVar: "MONGODB_TLS_CERT_FILE"},
		cli.StringFlag{Name: commonschema.CLIFlagTLSKeyFile, Usage: "MongoDB TLS client key path", EnvVar: "MONGODB_TLS_KEY_FILE"},
		cli.StringFlag{Name: commonschema.CLIFlagTLSCaFile, Usage: "MongoDB TLS CA file", EnvVar: "MONGODB_TLS_CA_FILE"},
		cli.StringFlag{Name: commonschema.CLIFlagTLSHostName, Usage: "override the MongoDB TLS server name", EnvVar: "MONGODB_TLS_SERVER_NAME"},
		cli.BoolFlag{Name: commonschema.CLIFlagTLSDisableHostVerification, Usage: "disable MongoDB TLS host name verification", EnvVar: "MONGODB_TLS_DISABLE_HOST_VERIFICATION"},
	}
	app.Commands = []cli.Command{
		{
			Name:  "setup-schema",
			Usage: "initialize MongoDB schema version metadata",
			Flags: []cli.Flag{cli.StringFlag{Name: flagVersion + ", v", Value: "0.0"}},
			Action: func(c *cli.Context) error {
				return withConnection(c, func(ctx context.Context, connection *Connection) error {
					return connection.SetupSchema(ctx, c.String(flagVersion))
				})
			},
		},
		{
			Name:  "update-schema",
			Usage: "apply MongoDB schema versions in ascending order",
			Flags: []cli.Flag{
				cli.StringFlag{Name: flagVersion + ", v"},
				cli.StringFlag{Name: flagSchemaDir},
				cli.StringFlag{Name: flagSchemaName, Value: defaultSchemaDir},
			},
			Action: updateSchema,
		},
		{
			Name:  "version",
			Usage: "print installed MongoDB schema version metadata",
			Action: func(c *cli.Context) error {
				return withConnection(c, func(ctx context.Context, connection *Connection) error {
					version, err := connection.ReadSchemaVersion(ctx)
					if err != nil {
						return err
					}
					_, err = fmt.Fprintf(c.App.Writer, "CurrentVersion: %s\nMinCompatibleVersion: %s\n", version.CurrentVersion, version.MinCompatibleVersion)
					return err
				})
			},
		},
	}
	return app
}

func updateSchema(c *cli.Context) error {
	schemaDir := strings.TrimSpace(c.String(flagSchemaDir))
	schemaName := strings.TrimSpace(c.String(flagSchemaName))
	if schemaDir != "" && c.IsSet(flagSchemaName) {
		return fmt.Errorf("only one of --%s and --%s may be set", flagSchemaDir, flagSchemaName)
	}

	var schemaFS fs.FS
	var versionedDir string
	if schemaDir != "" {
		schemaFS = os.DirFS(schemaDir)
		versionedDir = "."
	} else {
		schemaFS = dbschemas.Assets()
		versionedDir = schemaName + "/versioned"
	}
	return withConnection(c, func(ctx context.Context, connection *Connection) error {
		if err := NewUpdater(connection).Run(ctx, schemaFS, versionedDir, c.String(flagVersion)); err != nil {
			return err
		}
		return NewShardingInstaller(connection).Run(ctx)
	})
}

func withConnection(c *cli.Context, fn func(context.Context, *Connection) error) error {
	cfg, err := connectionConfig(c)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	connection, err := NewConnection(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		_ = connection.Close(closeCtx)
	}()
	return fn(ctx, connection)
}

func connectionConfig(c *cli.Context) (config.MongoDB, error) {
	uri := strings.TrimSpace(c.GlobalString(flagURI))
	if uri != "" && c.GlobalIsSet(flagEndpoint) {
		return config.MongoDB{}, fmt.Errorf("only one of --%s and --%s may be set", flagURI, flagEndpoint)
	}
	var hosts []string
	if uri == "" {
		for _, host := range strings.Split(c.GlobalString(flagEndpoint), ",") {
			if host = strings.TrimSpace(host); host != "" {
				hosts = append(hosts, host)
			}
		}
	}
	if uri == "" && len(hosts) == 0 {
		return config.MongoDB{}, errors.New("MongoDB endpoint is required")
	}
	if c.GlobalString(flagDatabase) == "" {
		return config.MongoDB{}, errors.New("MongoDB database is required")
	}
	replicaSet := ""
	if uri == "" || c.GlobalIsSet(flagReplicaSet) {
		replicaSet = c.GlobalString(flagReplicaSet)
	}
	var tlsConfig *auth.TLS
	if c.GlobalBool(commonschema.CLIFlagEnableTLS) {
		tlsConfig = &auth.TLS{
			Enabled:                true,
			CertFile:               c.GlobalString(commonschema.CLIFlagTLSCertFile),
			KeyFile:                c.GlobalString(commonschema.CLIFlagTLSKeyFile),
			CaFile:                 c.GlobalString(commonschema.CLIFlagTLSCaFile),
			EnableHostVerification: !c.GlobalBool(commonschema.CLIFlagTLSDisableHostVerification),
			ServerName:             c.GlobalString(commonschema.CLIFlagTLSHostName),
		}
	}
	return config.MongoDB{
		URI:            uri,
		Hosts:          hosts,
		User:           c.GlobalString(flagUser),
		Password:       c.GlobalString(flagPassword),
		DatabaseName:   c.GlobalString(flagDatabase),
		AuthSource:     c.GlobalString(flagAuthSource),
		ReplicaSet:     replicaSet,
		ConnectTimeout: 30 * time.Second,
		TLS:            tlsConfig,
	}, nil
}
