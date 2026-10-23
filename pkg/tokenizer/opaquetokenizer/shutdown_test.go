package opaquetokenizer

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/authz/openfgaserver"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/modules/organization"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlitesqlstore"
	"github.com/SigNoz/signoz/pkg/tokenizer"
	"github.com/SigNoz/signoz/pkg/tokenizer/tokenizerstore/sqltokenizerstore"
	"github.com/SigNoz/signoz/pkg/types"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"
)

func TestShutdownFlushAfterOpenFGAStops(t *testing.T) {
	ctx := context.Background()
	settings := instrumentationtest.New().ToProviderSettings()
	store, err := sqlitesqlstore.New(ctx, settings, sqlstore.Config{
		Provider: "sqlite",
		Sqlite: sqlstore.SqliteConfig{
			Path:            filepath.Join(t.TempDir(), "shutdown.db"),
			Mode:            "wal",
			BusyTimeout:     time.Second,
			TransactionMode: "immediate",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.BunDB().Close()) })
	_, err = store.BunDB().NewCreateTable().Model(new(authtypes.StorableToken)).Exec(ctx)
	require.NoError(t, err)
	// Only the columns used by TokenStore's organization join are needed here.
	_, err = store.SQLDB().ExecContext(ctx, "CREATE TABLE users (id TEXT PRIMARY KEY, org_id TEXT NOT NULL)")
	require.NoError(t, err)
	org := &types.Organization{Identifiable: types.Identifiable{ID: valuer.GenerateUUID()}}
	userID := valuer.GenerateUUID()
	_, err = store.SQLDB().ExecContext(ctx, "INSERT INTO users (id, org_id) VALUES (?, ?)", userID, org.ID)
	require.NoError(t, err)
	tokenStore := sqltokenizerstore.NewStore(store)
	activeToken, err := authtypes.NewToken(map[string]string{}, userID)
	require.NoError(t, err)
	require.NoError(t, tokenStore.Create(ctx, activeToken))

	observedAtCache, err := ristretto.NewCache(&ristretto.Config[string, time.Time]{NumCounters: 100, MaxCost: 1024, BufferItems: 64})
	require.NoError(t, err)
	t.Cleanup(observedAtCache.Close)
	observedAt := time.Now().UTC().Truncate(time.Millisecond)
	require.True(t, observedAtCache.Set(lastObservedAtCacheKey(activeToken.AccessToken, userID), observedAt, 24))
	observedAtCache.Wait()
	orgGetter := &shutdownOrgGetter{org: org, entered: make(chan struct{}), release: make(chan struct{})}
	provider := &provider{
		config:              tokenizer.Config{Lifetime: tokenizer.LifetimeConfig{Idle: time.Hour, Max: time.Hour}},
		settings:            factory.NewScopedProviderSettings(settings, "shutdown-test"),
		tokenStore:          tokenStore,
		orgGetter:           orgGetter,
		stopC:               make(chan struct{}),
		lastObservedAtCache: observedAtCache,
	}
	var releaseWrites sync.Once
	var stopProvider sync.Once
	t.Cleanup(func() {
		releaseWrites.Do(func() { close(orgGetter.release) })
		stopProvider.Do(func() { require.NoError(t, provider.Stop(ctx)) })
	})

	datastore, err := openfgaserver.NewSQLStore(store, authz.Config{})
	require.NoError(t, err)
	authorizer, err := openfgaserver.NewOpenfgaServer(ctx, settings, authz.Config{}, store, nil, datastore)
	require.NoError(t, err)
	var stopAuthorizer sync.Once
	t.Cleanup(func() { stopAuthorizer.Do(func() { require.NoError(t, authorizer.Stop(ctx)) }) })

	stopped := make(chan error, 1)
	go func() {
		stopProvider.Do(func() { stopped <- provider.Stop(ctx) })
	}()
	select {
	case <-orgGetter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("tokenizer did not reach shutdown cleanup")
	}
	stopAuthorizer.Do(func() { err = authorizer.Stop(ctx) })
	releaseWrites.Do(func() { close(orgGetter.release) })
	require.NoError(t, err)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("tokenizer did not finish shutdown")
	}

	// Stop logs cleanup errors, so check the committed data rather than only
	// its return value: pending activity must be flushed to the shared database.
	restoredToken, err := tokenStore.GetByAccessToken(ctx, activeToken.AccessToken)
	require.NoError(t, err)
	require.True(t, observedAt.Equal(restoredToken.LastObservedAt))
}

type shutdownOrgGetter struct {
	organization.Getter
	org     *types.Organization
	entered chan struct{}
	release chan struct{}
}

func (g *shutdownOrgGetter) ListByOwnedKeyRange(context.Context) ([]*types.Organization, error) {
	close(g.entered)
	<-g.release
	return []*types.Organization{g.org}, nil
}
