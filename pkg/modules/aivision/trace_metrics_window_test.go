package aivision

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

func runMetricsWindowRequest(t *testing.T, setup func(sqlmock.Sqlmock)) map[string]any {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	setup(mock)
	cfg := config{Enabled: true, QueryTimeout: 30 * time.Second, MaxQueryRange: time.Hour, TablePrefix: "test"}
	h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/metrics-window?from=100&to=200&space_id=space&user=owner&agent_product=product&agent_uuid=agent&projection_context="+strings.Repeat("a", 64)+"&raw=false", nil)
	request = request.WithContext(authtypes.NewContextWithClaims(request.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	response := httptest.NewRecorder()
	h.ReadModel(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NoError(t, mock.ExpectationsWereMet())
	var body struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	return body.Data
}

// A large canonical IO representation must not force an otherwise small metrics
// response to rebuild all raw spans. SQL is the only replaced external boundary.
func TestMetricsWindowKeepsFactsAndDropsContentBeforeWireBudget(t *testing.T) {
	result := runMetricsWindowRequest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		expectProjectionMaintenance(mock, true, false)
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
		mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "source_revision"}).
			AddRow("abc", 1, "agent", `{"trace_id":"abc","name":"null is preserved","userId":null,"span_count":1,"usage":{"total":9007199254740993},"_metrics":{"version":"native-trace-metrics-v1","observations":[]},"attributes.gen_ai.input.messages":"`+strings.Repeat("x", 8192)+`","ioPreview":{"input":"old content"},"arbitrary.payload":"not metrics"}`, 3))
		mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(addWindowSpan(windowSpanRows(), "def"))
		mock.ExpectCommit()
	})
	require.Equal(t, "native-trace-metrics-window-v1", result["contract"])
	meta := result["meta"].(map[string]any)
	require.Equal(t, "metrics-v1", meta["representation"])
	require.Equal(t, false, meta["raw"])
	require.EqualValues(t, 2, meta["spanCount"])
	rows := result["rows"].([]any)
	require.Len(t, rows, 1)
	row := rows[0].(map[string]any)
	require.Contains(t, row, "userId")
	require.Nil(t, row["userId"])
	require.NotContains(t, row, "environment")
	require.NotContains(t, row, "attributes.gen_ai.input.messages")
	require.NotContains(t, row, "ioPreview")
	require.NotContains(t, row, "arbitrary.payload")
	require.Contains(t, row, "_metrics")
	require.EqualValues(t, 3, row["sourceRevision"])
	require.Len(t, result["raw"].([]any), 1)
}

func TestMetricsWindowMaintenanceOffRetainsCompleteRawFallback(t *testing.T) {
	result := runMetricsWindowRequest(t, func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		expectProjectionMaintenance(mock, false, false)
		mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
		mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(windowSpanRows(), "abc"))
		mock.ExpectCommit()
	})
	require.Equal(t, "native-trace-metrics-window-v1", result["contract"])
	require.Equal(t, "metrics-v1", result["meta"].(map[string]any)["representation"])
	require.Equal(t, true, result["meta"].(map[string]any)["raw"])
	require.Empty(t, result["rows"])
	require.Len(t, result["raw"].([]any), 1)
}

func TestMetricsWindowBudgetsOnlyCompactFactsAndStillBoundsRawFallback(t *testing.T) {
	for _, tc := range []struct {
		name, largeField  string
		rawLarge, wantRaw bool
	}{
		{"content_is_not_transferred", "attributes.gen_ai.input.messages", false, false},
		{"large_facts_retry_raw", "_metrics", false, true},
		{"large_facts_and_raw_fail_closed", "_metrics", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 12000, 12000))
			row, err := json.Marshal(map[string]any{"trace_id": "abc", "usage": map[string]any{"total": json.Number("9007199254740993")}, tc.largeField: strings.Repeat("x", 8192)})
			require.NoError(t, err)
			mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "source_revision"}).AddRow("abc", 1, "agent", string(row), 3))
			if tc.wantRaw {
				rawRows := windowSpanRows()
				if tc.rawLarge {
					rawRows.AddRow("abc", "span", `{}`, `{"input":"`+strings.Repeat("x", 8192)+`"}`, `[]`, `[]`, `{}`, "100000000", "100000001", 0)
				} else {
					addWindowSpan(rawRows, "abc")
				}
				mock.ExpectQuery("SELECT trace_id,").WillReturnRows(rawRows)
			} else {
				mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(windowSpanRows())
			}
			if tc.rawLarge {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			result, err := store.traceWindowRepresentation(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+4096, true)
			if tc.rawLarge {
				require.ErrorIs(t, err, errTraceWindowPayloadBudget)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				data := result.(map[string]any)
				require.Equal(t, tc.wantRaw, data["meta"].(map[string]any)["raw"])
				if !tc.wantRaw {
					compact := data["rows"].([]map[string]any)[0]
					require.Equal(t, json.Number("9007199254740993"), compact["usage"].(map[string]any)["total"])
					require.NotContains(t, compact, "attributes.gen_ai.input.messages")
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
