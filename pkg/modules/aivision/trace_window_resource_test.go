package aivision

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// These are resource failures from valid queries, not malformed input. Exercise
// the actual preflight and HTTP error writer; no raw payload read may follow.
func TestTraceWindowResourceBoundsReturn413(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		traces, spans, bytes, maxBytes int64
	}{
		{"trace_count", 50001, 50001, 1024, 1024},
		{"span_count", 1, 500001, 1024, 1024},
		{"source_bytes", 6000, 6000, 512*1024*1024 + 1, 128 * 1024},
		{"single_span", 1, 1, 16*1024*1024 + 1, 16*1024*1024 + 1},
	} {
		for _, metrics := range []bool{false, true} {
			name := tc.name + "/window"
			if metrics {
				name = tc.name + "/metrics-window"
			}
			t.Run(name, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT COUNT\\(DISTINCT BINARY trace_id\\)").WillReturnRows(
					sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(tc.traces, tc.spans, tc.bytes, tc.maxBytes))
				mock.ExpectRollback()
				result, err := store.traceWindowRepresentation(context.Background(), "org", windowTestParams(), true, traceWindowPayloadLimit, metrics)
				require.Error(t, err)
				require.Nil(t, result, "over-budget windows must not return partial success")
				w := httptest.NewRecorder()
				(&handler{}).writeBackendError(w, err)
				require.Equal(t, 413, w.Code, w.Body.String())
				var body map[string]any
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				require.Equal(t, "error", body["status"])
				require.Contains(t, body["error"], "512 MiB source")
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestTraceWindowInvalidScopeRemains400(t *testing.T) {
	p := windowTestParams()
	p.SpaceID = strings.Repeat("s", 256)
	store := &oceanBaseStore{}
	result, err := store.traceWindow(context.Background(), "org", p, true)
	require.Error(t, err)
	require.Nil(t, result)
	w := httptest.NewRecorder()
	(&handler{}).writeBackendError(w, err)
	require.Equal(t, 400, w.Code, w.Body.String())
}
