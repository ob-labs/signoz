package ingest

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Exercise the actual OTLP handler and transaction boundary, not a replacement
// insert function. Losing the typed error through wrapping must break the test.
func TestOTLPStorageFailureRetryClassification(t *testing.T) {
	for _, signal := range []string{"traces", "logs"} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
		}{
			{"lock timeout", &mysql.MySQLError{Number: 1205}, 503},
			{"deadlock", &mysql.MySQLError{Number: 1213}, 503},
			{"wrapped deadline", fmt.Errorf("storage: %w", context.DeadlineExceeded), 500},
			{"bad connection before transaction", driver.ErrBadConn, 500},
			{"canceled request", context.Canceled, 500},
			{"invalid data", &mysql.MySQLError{Number: 1406}, 500},
			{"permission denied", &mysql.MySQLError{Number: 1142}, 500},
			{"unknown", errors.New("database offline"), 500},
		} {
			t.Run(signal+"/"+tc.name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				ep := &endpoint{db: db, cfg: Config{InternalKey: "test-key", OrgID: "org", TablePrefix: "test", MaxBodyBytes: 1 << 20}, logger: testLogger(), tracesTable: "`test_traces`", logsTable: "`test_logs`"}
				// Begin errors are guaranteed to occur before any raw write.
				mock.ExpectBegin().WillReturnError(fmt.Errorf("driver: %w", tc.err))
				w := callStorageTestHandler(t, ep, signal)
				require.Equal(t, tc.status, w.Code, w.Body.String())
				if tc.status == 503 {
					require.Equal(t, "1", w.Header().Get("Retry-After"))
				} else {
					require.Empty(t, w.Header().Get("Retry-After"))
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func callStorageTestHandler(t *testing.T, ep *endpoint, signal string) *httptest.ResponseRecorder {
	t.Helper()
	var body []byte
	var err error
	if signal == "traces" {
		body, err = (&ptrace.JSONMarshaler{}).MarshalTraces(newTestTraces())
	} else {
		body, err = (&plog.JSONMarshaler{}).MarshalLogs(newTestLogs())
	}
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/v1/"+signal, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(internalKeyHeader, "test-key")
	w := httptest.NewRecorder()
	if signal == "traces" {
		ep.handleTraces(w, r)
	} else {
		ep.handleLogs(w, r)
	}
	return w
}

func TestOTLPTraceLockTimeoutRollsBackBeforeRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	ep := &endpoint{db: db, cfg: Config{InternalKey: "test-key", OrgID: "org", TablePrefix: "test", MaxBodyBytes: 1 << 20}, logger: testLogger(), tracesTable: "`test_traces`"}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version").WillReturnError(&mysql.MySQLError{Number: 1205})
	mock.ExpectRollback()
	w := callStorageTestHandler(t, ep, "traces")
	require.Equal(t, 503, w.Code, w.Body.String())
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOTLPLogsCommitFailureRemainsAmbiguous(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	ep := &endpoint{db: db, cfg: Config{InternalKey: "test-key", OrgID: "org", TablePrefix: "test", MaxBodyBytes: 1 << 20}, logger: testLogger(), tracesTable: "`test_traces`", logsTable: "`test_logs`"}
	mock.ExpectBegin()
	mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `test_logs`")).ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(context.DeadlineExceeded)
	w := callStorageTestHandler(t, ep, "logs")
	require.Equal(t, 500, w.Code, w.Body.String())
	require.Empty(t, w.Header().Get("Retry-After"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOTLPInvalidDataDoesNotBecomeRetryable(t *testing.T) {
	ep := &endpoint{cfg: Config{InternalKey: "test-key", MaxBodyBytes: 1024}, logger: testLogger()}
	for _, handler := range []http.HandlerFunc{ep.handleTraces, ep.handleLogs} {
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString("invalid json"))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(internalKeyHeader, "test-key")
		w := httptest.NewRecorder()
		handler(w, r)
		require.Equal(t, 400, w.Code)
		require.Empty(t, w.Header().Get("Retry-After"))
	}
}
