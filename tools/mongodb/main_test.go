package mongodb

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestConnectionConfigUsesReplicaSetFromURI(t *testing.T) {
	app := BuildCLIOptions()
	flags := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	for _, appFlag := range app.Flags {
		appFlag.Apply(flags)
	}
	require.NoError(t, flags.Parse([]string{"--uri", "mongodb+srv://cluster.example.com"}))

	cfg, err := connectionConfig(cli.NewContext(app, flags, nil))

	require.NoError(t, err)
	require.Empty(t, cfg.ReplicaSet)
}

func TestConnectionConfigUsesDefaultReplicaSetWithEndpoints(t *testing.T) {
	app := BuildCLIOptions()
	flags := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	for _, appFlag := range app.Flags {
		appFlag.Apply(flags)
	}

	cfg, err := connectionConfig(cli.NewContext(app, flags, nil))

	require.NoError(t, err)
	require.NotEmpty(t, cfg.ReplicaSet)
}
