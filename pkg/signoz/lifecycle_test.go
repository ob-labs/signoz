package signoz

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/cache"
	"github.com/SigNoz/signoz/pkg/errors"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/licensing"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlstoretest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/zeus"
	"github.com/SigNoz/signoz/pkg/zeus/noopzeus"
	"github.com/stretchr/testify/require"
)

type shutdownService struct {
	stop func(context.Context) error
}

func (service *shutdownService) Start(context.Context) error { return nil }

func (service *shutdownService) Stop(ctx context.Context) error { return service.stop(ctx) }

func TestStopClosesSQLStoreAfterServices(t *testing.T) {
	serviceErr := errors.NewInternalf(errors.CodeInternal, "service shutdown failed")
	closeErr := errors.NewInternalf(errors.CodeInternal, "SQL close failed")
	for _, test := range []struct {
		name       string
		serviceErr error
		closeErr   error
	}{
		{name: "success"},
		{name: "service error", serviceErr: serviceErr},
		{name: "close error", closeErr: closeErr},
		{name: "both errors", serviceErr: serviceErr, closeErr: closeErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			store := sqlstoretest.New(sqlstore.Config{Provider: "sqlite"}, sqlmock.QueryMatcherEqual)
			store.Mock().ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
			store.Mock().ExpectClose().WillReturnError(test.closeErr)
			t.Cleanup(func() { _ = store.BunDB().Close() })

			stopping := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseService := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseService()
			var stopCalls atomic.Int32
			consumerResult := make(chan error, 1)
			consumer := &shutdownService{stop: func(ctx context.Context) error {
				stopCalls.Add(1)
				close(stopping)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				var value int
				err := store.SQLDB().QueryRowContext(ctx, "SELECT 1").Scan(&value)
				consumerResult <- err
				return errors.Join(err, test.serviceErr)
			}}
			registry, err := factory.NewRegistry(ctx, slog.New(slog.DiscardHandler), factory.NewNamedService(factory.MustNewName("consumer"), consumer))
			require.NoError(t, err)
			instance := &SigNoz{Registry: registry, SQLStore: store}

			const callers = 4
			results := make(chan error, callers)
			for range callers {
				go func() { results <- instance.Stop(ctx) }()
			}
			select {
			case <-stopping:
			case <-ctx.Done():
				t.Fatal("consumer never started stopping")
			}

			// A consumer can finish SQL work while shutdown is waiting for it.
			require.NoError(t, store.SQLDB().PingContext(ctx))
			releaseService()
			for range callers {
				select {
				case err := <-results:
					if test.serviceErr == nil && test.closeErr == nil {
						require.NoError(t, err)
					} else {
						if test.serviceErr != nil {
							require.ErrorIs(t, err, test.serviceErr)
						}
						if test.closeErr != nil {
							require.ErrorIs(t, err, test.closeErr)
						}
					}
				case <-ctx.Done():
					t.Fatal("shutdown did not finish")
				}
			}
			require.NoError(t, <-consumerResult)
			require.Equal(t, int32(1), stopCalls.Load())
			require.ErrorContains(t, store.SQLDB().PingContext(ctx), "database is closed")
			require.NoError(t, store.Mock().ExpectationsWereMet())
		})
	}
}

func TestNewClosesSQLStoreOnConstructionFailure(t *testing.T) {
	ctx := context.Background()
	startupErr := errors.NewInternalf(errors.CodeInternal, "telemetry store initialization failed")
	closeErr := errors.NewInternalf(errors.CodeInternal, "SQL close failed")
	store := sqlstoretest.New(sqlstore.Config{Provider: "sqlite"}, sqlmock.QueryMatcherEqual)
	store.Mock().ExpectClose().WillReturnError(closeErr)
	t.Cleanup(func() { _ = store.BunDB().Close() })

	config := Config{
		Cache:          cache.Config{Provider: "test"},
		SQLStore:       sqlstore.Config{Provider: "sqlite"},
		TelemetryStore: telemetrystore.Config{Provider: "test"},
	}
	cacheFactories := factory.MustNewNamedMap(factory.NewProviderFactory(factory.MustNewName("test"), func(context.Context, factory.ProviderSettings, cache.Config) (cache.Cache, error) {
		// Construction fails before any component uses the cache.
		return &struct{ cache.Cache }{}, nil
	}))
	storeFactories := factory.MustNewNamedMap(factory.NewProviderFactory(factory.MustNewName("sqlite"), func(context.Context, factory.ProviderSettings, sqlstore.Config) (sqlstore.SQLStore, error) {
		return store, nil
	}))
	telemetryFactories := factory.MustNewNamedMap(factory.NewProviderFactory(factory.MustNewName("test"), func(context.Context, factory.ProviderSettings, telemetrystore.Config) (telemetrystore.TelemetryStore, error) {
		return nil, startupErr
	}))

	// Factories after the telemetry store are not reached on this failure path.
	instance, err := New(ctx, config, zeus.Config{}, noopzeus.NewProviderFactory(), licensing.Config{}, nil, nil,
		NewEmailingProviderFactories(), cacheFactories, NewWebProviderFactories(config.Global), nil,
		storeFactories, telemetryFactories, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.Nil(t, instance)
	require.ErrorIs(t, err, startupErr)
	require.ErrorIs(t, err, closeErr)
	require.ErrorContains(t, store.SQLDB().PingContext(ctx), "database is closed")
	require.NoError(t, store.Mock().ExpectationsWereMet())
}
