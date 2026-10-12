package testcore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
)

func TestOneboxStopsDependenciesAfterWorkerFollowingRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			host := &TemporalImpl{
				serviceApps:            make(map[primitives.ServiceName][]*fx.App),
				namespaceRegistryByApp: make(map[*fx.App]namespace.Registry),
			}
			running := make(map[primitives.ServiceName]bool)
			var stopped []primitives.ServiceName
			start := func(service primitives.ServiceName) {
				app := fx.New(fx.NopLogger, fx.Invoke(func(lifecycle fx.Lifecycle) {
					lifecycle.Append(fx.Hook{
						OnStart: func(context.Context) error {
							running[service] = true
							return nil
						},
						OnStop: func(context.Context) error {
							if service == primitives.WorkerService {
								for _, dependency := range []primitives.ServiceName{
									primitives.FrontendService, primitives.HistoryService, primitives.MatchingService,
								} {
									if !running[dependency] {
										return fmt.Errorf("worker dependency %s stopped before worker", dependency)
									}
								}
							}
							running[service] = false
							stopped = append(stopped, service)
							return nil
						},
					})
				}))
				require.NoError(t, app.Start(context.Background()))
				host.addServiceApp(service, app, nil)
			}
			for _, service := range []primitives.ServiceName{
				primitives.MatchingService, primitives.HistoryService, primitives.FrontendService, primitives.WorkerService,
			} {
				start(service)
			}
			if restart {
				require.NoError(t, host.StopService(context.Background(), primitives.HistoryService))
				require.NoError(t, host.StopService(context.Background(), primitives.MatchingService))
				start(primitives.HistoryService)
				start(primitives.MatchingService)
				stopped = nil
			}
			require.NoError(t, host.Stop())
			require.Equal(t, []primitives.ServiceName{
				primitives.WorkerService, primitives.FrontendService, primitives.HistoryService, primitives.MatchingService,
			}, stopped)
		})
	}
}
