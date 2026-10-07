package environment

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLookupLocalhostIPSuccess(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ipString := lookupLocalhostIP("localhost")
	ip := net.ParseIP(ipString)
	// localhost needs to resolve to a loopback address
	// whether it's ipv4 or ipv6 - the result depends on
	// the system running this test
	r.True(ip.IsLoopback())
}

func TestLookupLocalhostIPMissingHostname(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ipString := lookupLocalhostIP("")
	ip := net.ParseIP(ipString)
	r.True(ip.IsLoopback())
	// if host can't be found, use ipv4 loopback
	r.Equal(localhostIPDefault, ip.String())
}

func TestLookupLocalhostIPWithIPv6(t *testing.T) {
	t.Parallel()
	r := require.New(t)
	ipString := lookupLocalhostIP("::1")
	ip := net.ParseIP(ipString)
	r.True(ip.IsLoopback())
	// return ipv6 if only ipv6 is available
	r.Equal(ip, net.ParseIP("::1"))
}

func TestGetMongoDBReplicaSet(t *testing.T) {
	t.Run("configured replica set", func(t *testing.T) {
		t.Setenv(mongodbReplicaSetEnv, "qualification-rs")
		require.Equal(t, "qualification-rs", GetMongoDBReplicaSet())
	})

	t.Run("explicit empty selects mongos", func(t *testing.T) {
		t.Setenv(mongodbReplicaSetEnv, "")
		require.Empty(t, GetMongoDBReplicaSet())
	})
}
