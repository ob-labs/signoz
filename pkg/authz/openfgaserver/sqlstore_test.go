package openfgaserver

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlitesqlstore"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func realSharedSQLStore(t *testing.T) sqlstore.SQLStore {
	t.Helper()
	store, err := sqlitesqlstore.New(context.Background(), instrumentationtest.New().ToProviderSettings(), sqlstore.Config{
		Provider:   "sqlite",
		Connection: sqlstore.ConnectionConfig{MaxOpenConns: 4},
		Sqlite: sqlstore.SqliteConfig{
			Path: filepath.Join(t.TempDir(), "metadata.db"), Mode: "wal",
			BusyTimeout: time.Second, TransactionMode: "immediate",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.SQLDB().Close()) })
	_, err = store.SQLDB().Exec("CREATE TABLE shutdown_snapshot (value TEXT NOT NULL)")
	require.NoError(t, err)
	return store
}

func sharedSQLServer(t *testing.T, store sqlstore.SQLStore) *Server {
	t.Helper()
	config := authz.Config{OpenFGA: authz.OpenFGAConfig{MaxTuplesPerWrite: 100}}
	datastore, err := NewSQLStore(store, config)
	require.NoError(t, err)
	server, err := NewOpenfgaServer(context.Background(), instrumentationtest.New().ToProviderSettings(), config, store,
		[]transformer.ModuleFile{{Name: "test.fga", Contents: "module base\ntype user"}}, datastore)
	require.NoError(t, err)
	return server
}

func TestAuthzStopPreservesSharedPoolForFinalShutdownWrite(t *testing.T) {
	store := realSharedSQLStore(t)
	// The two metadata tables used by real OpenFGA startup. The remaining
	// consumer is independent of authorization and writes its own final state.
	for _, statement := range []string{
		"CREATE TABLE store (id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP, deleted_at TIMESTAMP)",
		"CREATE TABLE authorization_model (store TEXT NOT NULL, authorization_model_id TEXT NOT NULL, schema_version TEXT NOT NULL, serialized_protobuf BLOB NOT NULL, PRIMARY KEY (store, authorization_model_id))",
	} {
		_, err := store.SQLDB().Exec(statement)
		require.NoError(t, err)
	}
	server := sharedSQLServer(t, store)
	started := make(chan error, 1)
	go func() { started <- server.Start(context.Background()) }()
	select {
	case <-server.Healthy():
	case err := <-started:
		t.Fatalf("authorization startup failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("authorization startup did not become healthy")
	}

	stopped := make(chan struct{})
	finalWrite := make(chan error, 1)
	go func() {
		<-stopped
		_, err := store.SQLDB().Exec("INSERT INTO shutdown_snapshot VALUES (?)", "final state")
		finalWrite <- err
	}()
	require.NoError(t, server.Stop(context.Background()))
	// This barrier forces authorization cleanup to finish first. It cannot
	// pass merely because the other service happened to win a shutdown race.
	close(stopped)
	require.NoError(t, <-finalWrite, "another consumer must retain SQL access after authorization stops")
	require.NoError(t, <-started)
	var value string
	require.NoError(t, store.SQLDB().QueryRow("SELECT value FROM shutdown_snapshot").Scan(&value))
	require.Equal(t, "final state", value)

	// The platform owner still controls the real pool's final release.
	require.NoError(t, store.SQLDB().Close())
	require.ErrorContains(t, store.SQLDB().Ping(), "database is closed")
}

func TestBorrowedDatastoreCanCloseAndRecreateWithoutClosingSharedPool(t *testing.T) {
	store := realSharedSQLStore(t)
	for i := 0; i < 3; i++ {
		datastore, err := NewSQLStore(store, authz.Config{})
		require.NoError(t, err)
		datastore.Close()
		datastore.Close()
		_, err = store.SQLDB().Exec("INSERT INTO shutdown_snapshot VALUES (?)", fmt.Sprint(i))
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, store.SQLDB().QueryRow("SELECT count(*) FROM shutdown_snapshot").Scan(&count))
	require.Equal(t, 3, count)
}

func TestBorrowedDatastoreDoesNotRegisterOwnedPoolMetrics(t *testing.T) {
	store := realSharedSQLStore(t)
	registry := prometheus.NewRegistry()
	previousRegisterer, previousGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
	prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
	t.Cleanup(func() {
		prometheus.DefaultRegisterer, prometheus.DefaultGatherer = previousRegisterer, previousGatherer
	})
	for i := 0; i < 2; i++ {
		datastore, err := NewSQLStore(store, authz.Config{})
		require.NoError(t, err)
		metrics, err := registry.Gather()
		require.NoError(t, err)
		require.Empty(t, metrics, "borrowed datastore must not acquire a DBStatsCollector that needs Close cleanup")
		datastore.Close()
	}
	metrics, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metrics)
}

type storeWithUnsupportedDialect struct {
	sqlstore.SQLStore
	bunDB *bun.DB
}

func (store storeWithUnsupportedDialect) BunDB() *bun.DB { return store.bunDB }

func TestFailedInitializationDoesNotCloseSharedPool(t *testing.T) {
	t.Run("unsupported store", func(t *testing.T) {
		store := realSharedSQLStore(t)
		_, err := NewSQLStore(storeWithUnsupportedDialect{SQLStore: store, bunDB: bun.NewDB(store.SQLDB(), pgdialect.New())}, authz.Config{})
		require.Error(t, err)
		require.NoError(t, store.SQLDB().Ping())
	})
	t.Run("startup failure", func(t *testing.T) {
		store := realSharedSQLStore(t)
		server := sharedSQLServer(t, store)
		// No OpenFGA schema was created: startup fails after constructing the
		// server and its caches, and the normal cleanup path must remain safe.
		require.Error(t, server.Start(context.Background()))
		require.NoError(t, server.Stop(context.Background()))
		require.NoError(t, server.Stop(context.Background()))
		_, err := store.SQLDB().Exec("INSERT INTO shutdown_snapshot VALUES (?)", "startup failed")
		require.NoError(t, err)
	})
}

func TestServerStopIsConcurrentAndIdempotent(t *testing.T) {
	store := realSharedSQLStore(t)
	server := sharedSQLServer(t, store)
	var stops sync.WaitGroup
	for i := 0; i < 8; i++ {
		stops.Add(1)
		go func() {
			defer stops.Done()
			require.NoError(t, server.Stop(context.Background()))
		}()
	}
	stops.Wait()
	require.NoError(t, store.SQLDB().Ping())
}
