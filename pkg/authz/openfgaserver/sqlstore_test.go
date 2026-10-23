package openfgaserver

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlitesqlstore"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/stretchr/testify/require"
)

func newSharedSQLStore(t *testing.T) sqlstore.SQLStore {
	t.Helper()
	config := sqlstore.NewConfigFactory().New().(sqlstore.Config)
	config.Sqlite.Path = filepath.Join(t.TempDir(), "metadata.db")
	store, err := sqlitesqlstore.New(context.Background(), instrumentationtest.New().ToProviderSettings(), config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.BunDB().Close()) })
	return store
}

func TestSQLStoreClosePreservesSharedPool(t *testing.T) {
	ctx := context.Background()
	store := newSharedSQLStore(t)
	datastore, err := NewSQLStore(store, authz.Config{OpenFGA: authz.OpenFGAConfig{MaxTuplesPerWrite: 42}})
	require.NoError(t, err)
	require.Equal(t, 42, datastore.MaxTuplesPerWrite())

	_, err = store.SQLDB().ExecContext(ctx, "CREATE TABLE shutdown_state (value TEXT)")
	require.NoError(t, err)
	datastore.Close()
	datastore.Close()

	// Another consumer's final write must succeed even after authz has closed.
	_, err = store.SQLDB().ExecContext(ctx, "INSERT INTO shutdown_state VALUES ('persisted')")
	require.NoError(t, err)
	var value string
	require.NoError(t, store.SQLDB().QueryRowContext(ctx, "SELECT value FROM shutdown_state").Scan(&value))
	require.Equal(t, "persisted", value)

	// The owner still controls the real pool lifetime.
	require.NoError(t, store.BunDB().Close())
	require.ErrorContains(t, store.SQLDB().PingContext(ctx), "database is closed")
}

type countedDatastore struct {
	storage.OpenFGADatastore
	closes atomic.Int32
}

func (store *countedDatastore) Close() {
	store.closes.Add(1)
	store.OpenFGADatastore.Close()
}

func TestServerRejectsMissingDatastore(t *testing.T) {
	server, err := NewOpenfgaServer(context.Background(), instrumentationtest.New().ToProviderSettings(), authz.Config{}, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, server)
}

func TestServerFailedStartReleasesResourcesWithoutClosingSharedPool(t *testing.T) {
	for _, stage := range []string{"store", "model"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			store := newSharedSQLStore(t)
			if stage == "model" {
				// Store initialization succeeds, but the model tables are missing.
				_, err := store.SQLDB().ExecContext(ctx, `CREATE TABLE store (
					id TEXT PRIMARY KEY, name TEXT, created_at DATETIME,
					updated_at DATETIME, deleted_at DATETIME)`)
				require.NoError(t, err)
			}
			datastore, err := NewSQLStore(store, authz.Config{})
			require.NoError(t, err)
			counted := &countedDatastore{OpenFGADatastore: datastore}
			server, err := NewOpenfgaServer(ctx, instrumentationtest.New().ToProviderSettings(), authz.Config{}, store,
				[]transformer.ModuleFile{{Name: "test.fga", Contents: "module base\ntype user"}}, counted)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Stop(ctx)) })

			err = server.Start(ctx)
			require.Error(t, err)
			require.EqualValues(t, 1, counted.closes.Load(), "failed initialization must release OpenFGA resources")
			select {
			case <-server.Healthy():
				t.Fatal("failed server must not become healthy")
			default:
			}
			require.NoError(t, server.Stop(ctx))
			require.EqualValues(t, 1, counted.closes.Load())
			require.NoError(t, store.SQLDB().PingContext(ctx))
		})
	}
}

func TestServerStopReleasesOwnedDatastoreOnce(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "before_start"
		if started {
			name = "after_start"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			// The server must still close datastores it owns, unlike the borrowed
			// SQLite adapter. Exercise real OpenFGA caches and datastore cleanup.
			datastore := &countedDatastore{OpenFGADatastore: memory.New()}
			server, err := NewOpenfgaServer(ctx, instrumentationtest.New().ToProviderSettings(), authz.Config{}, nil,
				[]transformer.ModuleFile{{Name: "test.fga", Contents: "module base\ntype user"}}, datastore)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Stop(ctx)) })

			startResult := make(chan error, 1)
			if started {
				go func() { startResult <- server.Start(ctx) }()
				select {
				case <-server.Healthy():
				case err := <-startResult:
					t.Fatalf("server failed to start: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}

			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() { _ = server.Stop(ctx) })
			}
			wg.Wait()
			require.EqualValues(t, 1, datastore.closes.Load())
			if started {
				select {
				case err := <-startResult:
					require.NoError(t, err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
