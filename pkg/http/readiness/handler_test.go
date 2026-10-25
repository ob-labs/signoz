package readiness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
)

// The legacy ClickHouse handle is deliberately unusable, as in the OceanBase
// provider. The probe must use the configured provider's portable query API.
type probeStore struct {
	telemetrystore.TelemetryStore
	query func(context.Context, string, ...any) (telemetrystore.Rows, error)
}

func (s probeStore) QueryContext(ctx context.Context, query string, args ...any) (telemetrystore.Rows, error) {
	return s.query(ctx, query, args...)
}
func (probeStore) ClickhouseDB() clickhouse.Conn {
	panic("legacy ClickHouse connection used by readiness")
}

type probeRows struct {
	telemetrystore.Rows
	value                     uint8
	empty                     bool
	scanErr, rowErr, closeErr error
	closed, read              bool
}

func (r *probeRows) Next() bool {
	if r.empty || r.read {
		return false
	}
	r.read = true
	return true
}
func (r *probeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	*dest[0].(*uint8) = r.value
	return nil
}
func (r *probeRows) Err() error   { return r.rowErr }
func (r *probeRows) Close() error { r.closed = true; return r.closeErr }

func TestReadinessUsesPortableReadOnlyQuery(t *testing.T) {
	rows := &probeRows{value: 1}
	store := probeStore{query: func(ctx context.Context, query string, args ...any) (telemetrystore.Rows, error) {
		if query != "SELECT 1" || len(args) != 0 {
			t.Fatalf("unexpected readiness SQL: %q %v", query, args)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("probe has no short deadline")
		}
		return rows, nil
	}}
	response := httptest.NewRecorder()
	NewHandler(store).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil))
	if response.Code != http.StatusOK || response.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("unexpected readiness response: %d %s", response.Code, response.Body.String())
	}
	if !rows.closed {
		t.Fatal("probe result was not closed")
	}
}

func TestReadinessFailuresDoNotExposeDatabaseDetails(t *testing.T) {
	secret := errors.New("mysql://root:secret-password@private-db:2881/signoz")
	for name, rows := range map[string]*probeRows{
		"empty": {empty: true}, "wrong scalar": {value: 0},
		"scan error": {scanErr: secret}, "row error": {value: 1, rowErr: secret},
		"close error": {value: 1, closeErr: secret},
	} {
		t.Run(name, func(t *testing.T) {
			response := httptest.NewRecorder()
			NewHandler(probeStore{query: func(context.Context, string, ...any) (telemetrystore.Rows, error) { return rows, nil }}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil))
			assertUnavailable(t, response)
			if !rows.closed {
				t.Fatal("failed probe result was not closed")
			}
		})
	}
	response := httptest.NewRecorder()
	NewHandler(probeStore{query: func(context.Context, string, ...any) (telemetrystore.Rows, error) { return nil, secret }}).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil))
	assertUnavailable(t, response)
}

func TestReadinessBoundsUnresponsiveDatabase(t *testing.T) {
	handler := NewHandler(probeStore{query: func(ctx context.Context, _ string, _ ...any) (telemetrystore.Rows, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	started := time.Now()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil))
	assertUnavailable(t, response)
	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("probe took %s; want one-second deadline", elapsed)
	}
}

func TestReadinessPropagatesRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, done := make(chan struct{}), make(chan struct{})
	handler := NewHandler(probeStore{query: func(ctx context.Context, _ string, _ ...any) (telemetrystore.Rows, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	response := httptest.NewRecorder()
	go func() {
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/ready", nil).WithContext(ctx))
		close(done)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("database probe did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("request cancellation did not stop the database probe")
	}
	assertUnavailable(t, response)
}

func assertUnavailable(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"status\":\"unavailable\"}\n" {
		t.Fatalf("unexpected failure response: %d %s", response.Code, response.Body.String())
	}
}
