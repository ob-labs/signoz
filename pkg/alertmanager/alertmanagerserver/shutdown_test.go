package alertmanagerserver

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/alertmanager/alertmanagerstore/sqlalertmanagerstore"
	"github.com/SigNoz/signoz/pkg/alertmanager/nfmanager/nfmanagertest"
	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/authz/openfgaserver"
	"github.com/SigNoz/signoz/pkg/errors"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlitesqlstore"
	"github.com/SigNoz/signoz/pkg/types/alertmanagertypes"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestShutdownSnapshotsAfterOpenFGAStops(t *testing.T) {
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
	_, err = store.BunDB().NewCreateTable().Model(new(alertmanagertypes.StoreableState)).Exec(ctx)
	require.NoError(t, err)
	_, err = store.SQLDB().ExecContext(ctx, "CREATE UNIQUE INDEX alertmanager_state_org_id ON alertmanager_state (org_id)")
	require.NoError(t, err)

	datastore, err := openfgaserver.NewSQLStore(store, authz.Config{})
	require.NoError(t, err)
	authorizer, err := openfgaserver.NewOpenfgaServer(ctx, settings, authz.Config{}, store, nil, datastore)
	require.NoError(t, err)
	var stopAuthorizer sync.Once
	t.Cleanup(func() { stopAuthorizer.Do(func() { require.NoError(t, authorizer.Stop(ctx)) }) })

	stateStore := &shutdownStateStore{
		StateStore: sqlalertmanagerstore.NewStateStore(store),
		writing:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	var releaseWrites sync.Once
	t.Cleanup(func() { releaseWrites.Do(func() { close(stateStore.release) }) })
	server, err := New(ctx, settings.Logger, prometheus.NewRegistry(), NewConfig(), "shutdown-org", stateStore, nfmanagertest.NewMock(), newTestMaintenanceStore())
	require.NoError(t, err)
	var stopServer sync.Once
	t.Cleanup(func() {
		releaseWrites.Do(func() { close(stateStore.release) })
		stopServer.Do(func() { require.NoError(t, server.Stop(ctx)) })
	})

	sil := &silencepb.Silence{
		Matchers:  []*silencepb.Matcher{{Type: silencepb.Matcher_EQUAL, Name: "alertname", Pattern: "shutdown"}},
		StartsAt:  time.Now(),
		EndsAt:    time.Now().Add(time.Hour),
		CreatedBy: "shutdown-test",
		Comment:   "persist during shutdown",
	}
	require.NoError(t, server.silences.Set(ctx, sil))
	receiver := &nflogpb.Receiver{GroupName: "shutdown", Integration: "webhook"}
	require.NoError(t, server.nflog.Log(receiver, "shutdown-group", []uint64{42}, []uint64{7}, nil, time.Hour))

	// Hold the first final SQL write until the real OpenFGA server has stopped.
	// This forces the failing order without relying on sleeps or scheduling luck.
	stateStore.armed.Store(true)
	stopped := make(chan error, 1)
	go func() {
		stopServer.Do(func() { stopped <- server.Stop(ctx) })
	}()
	select {
	case <-stateStore.writing:
	case <-time.After(5 * time.Second):
		t.Fatal("alertmanager did not reach its final snapshot write")
	}
	stopAuthorizer.Do(func() { err = authorizer.Stop(ctx) })
	releaseWrites.Do(func() { close(stateStore.release) })
	require.NoError(t, err)
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("alertmanager did not finish shutdown")
	}
	// Stop logs snapshot failures rather than returning them, so inspect the
	// actual SQL results as well as loading both snapshots into a fresh server.
	require.Empty(t, stateStore.failures)
	require.Equal(t, 2, stateStore.writes)
	require.NoError(t, store.SQLDB().PingContext(ctx))

	restarted, err := New(ctx, settings.Logger, prometheus.NewRegistry(), NewConfig(), "shutdown-org", stateStore.StateStore, nfmanagertest.NewMock(), newTestMaintenanceStore())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Stop(ctx)) })
	restoredSilence, err := restarted.silences.QueryOne(ctx, silence.QIDs(sil.Id))
	require.NoError(t, err)
	require.Equal(t, sil.Comment, restoredSilence.Comment)
	restoredLog, err := restarted.nflog.Query(nflog.QReceiver(receiver), nflog.QGroupKey("shutdown-group"))
	require.NoError(t, err)
	require.Len(t, restoredLog, 1)
	require.Equal(t, []uint64{42}, restoredLog[0].FiringAlerts)
	require.Equal(t, []uint64{7}, restoredLog[0].ResolvedAlerts)
}

// The two maintenance callbacks each read and replace the full state row. Keep
// those read/write pairs serial here to isolate database ownership from their
// separate lost-update race, while still overlapping shutdown with OpenFGA.
type shutdownStateStore struct {
	alertmanagertypes.StateStore
	armed    atomic.Bool
	mu       sync.Mutex
	first    sync.Once
	writing  chan struct{}
	release  chan struct{}
	writes   int
	failures []error
}

func (s *shutdownStateStore) Get(ctx context.Context, orgID string) (*alertmanagertypes.StoreableState, error) {
	if !s.armed.Load() {
		return s.StateStore.Get(ctx, orgID)
	}
	s.mu.Lock()
	state, err := s.StateStore.Get(ctx, orgID)
	if err != nil && !errors.Ast(err, errors.TypeNotFound) {
		s.failures = append(s.failures, err)
		s.mu.Unlock()
	}
	return state, err
}

func (s *shutdownStateStore) Set(ctx context.Context, state *alertmanagertypes.StoreableState) error {
	defer s.mu.Unlock()
	s.first.Do(func() { close(s.writing) })
	<-s.release
	err := s.StateStore.Set(ctx, state)
	s.writes++
	if err != nil {
		s.failures = append(s.failures, err)
	}
	return err
}
