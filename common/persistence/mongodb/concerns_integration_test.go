//go:build integration

package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
)

func TestFactoryRejectsUnsafePersistenceConcerns(t *testing.T) {
	primaryFactory, cfg := newFencingTestFactory(t, "transaction_concerns")
	cfg.ReadPreference = "primaryPreferred"
	_, err := NewFactory(cfg, "test-cluster", primaryFactory.logger, metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "expected primary")

	cfg.ReadPreference = "primary"
	cfg.WriteConcern = "w1"
	_, err = NewFactory(cfg, "test-cluster", primaryFactory.logger, metrics.NoopMetricsHandler)
	require.ErrorContains(t, err, "expected majority")
}
