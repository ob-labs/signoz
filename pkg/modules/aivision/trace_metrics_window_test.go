package aivision

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

// Unsupported typed-statistics engines still need bounded selected-event IO;
// carrying the published preview map must not reopen full raw IO transfer.
func TestMetricsWindowPreservesBoundedOperationPreviewsWithoutRawIO(t *testing.T) {
	previews := map[string]any{}
	for i := 0; i < 200; i++ {
		previews[fmt.Sprintf("%016x", i)] = map[string]any{"name": "operation", "latency": 1, "status": 1, "input": strings.Repeat("i", 1024), "output": strings.Repeat("o", 1024), "input_truncated": true, "output_truncated": true}
	}
	metrics := map[string]any{"version": "native-trace-metrics-v1", "observations": []any{}}
	row := map[string]any{"trace_id": "abc", "span_count": 200, "usage": map[string]any{"total": json.Number("9007199254740993")}, "_metrics": metrics, "_operation_previews": previews}
	for _, field := range []string{"input", "output", "attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "ioPreview", "payload"} {
		row[field] = strings.Repeat("raw", 1<<20)
	}
	compact := traceMetricsWindowRow(row)
	require.Contains(t, compact, "_operation_previews")
	require.Equal(t, previews, compact["_operation_previews"])
	require.Equal(t, metrics, compact["_metrics"])
	require.Equal(t, json.Number("9007199254740993"), compact["usage"].(map[string]any)["total"])
	for _, field := range []string{"input", "output", "attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "ioPreview", "payload"} {
		require.NotContains(t, compact, field)
	}
	wire, err := json.Marshal(compact)
	require.NoError(t, err)
	require.Less(t, len(wire), 2<<20)
}

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
		expectMetricsWindowStoredColumns(mock)
		mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(16384))
		mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,").WillReturnRows(storedMetricsWindowRows(t, `{"trace_id":"abc","name":"null is preserved","userId":null,"span_count":1,"usage":{"total":9007199254740993},"_metrics":{"version":"native-trace-metrics-v1","observations":[]},"attributes.gen_ai.input.messages":"`+strings.Repeat("x", 8192)+`","ioPreview":{"input":"old content"},"arbitrary.payload":"not metrics"}`))
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
			expectMetricsWindowStoredColumns(mock)
			mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(16384))
			row, err := json.Marshal(map[string]any{"trace_id": "abc", "usage": map[string]any{"total": json.Number("9007199254740993")}, tc.largeField: strings.Repeat("x", 8192)})
			require.NoError(t, err)
			mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,").WillReturnRows(storedMetricsWindowRows(t, string(row)))
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

func TestMetricsWindowHugeHistoricalProjectionFallsBackBeforeDecode(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
	expectMetricsWindowStoredColumns(mock)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(sourcePageTraceByteLimit + 1))
	// No canonical row is fetched, parsed or compacted before the raw retry.
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(windowSpanRows(), "abc"))
	mock.ExpectCommit()
	result, err := store.traceWindowRepresentation(context.Background(), "org", windowTestParams(), false, traceWindowPayloadLimit, true)
	require.NoError(t, err)
	page := result.(map[string]any)
	require.Empty(t, page["rows"])
	require.Len(t, page["raw"], 1)
	require.Equal(t, true, page["meta"].(map[string]any)["raw"])
	require.Equal(t, "canonical_summary_budget", page["meta"].(map[string]any)["recoveryReason"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMetricsWindowSourcePreflightPreservesMissingAndNullMetrics(t *testing.T) {
	for _, tc := range []struct {
		name, document string
		present        bool
	}{{"missing", `{"trace_id":"abc"}`, false}, {"explicit_null", `{"trace_id":"abc","_metrics":null}`, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
			expectMetricsWindowStoredColumns(mock)
			mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(len(tc.document)))
			mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,").WillReturnRows(storedMetricsWindowRows(t, tc.document))
			mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(windowSpanRows())
			mock.ExpectCommit()
			result, err := store.traceWindowRepresentation(context.Background(), "org", windowTestParams(), false, traceWindowPayloadLimit, true)
			require.NoError(t, err)
			row := result.(map[string]any)["rows"].([]map[string]any)[0]
			value, present := row["_metrics"]
			require.Equal(t, tc.present, present)
			require.Nil(t, value)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// The database must send compact facts, not full canonical IO, while missing
// keys, explicit nulls, observations and exact numbers retain their meaning.
func TestMetricsWindowConsumesCompactSQLFacts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
	expectMetricsWindowStoredColumns(mock)
	mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(16 << 20))
	mock.ExpectQuery(`SELECT r.trace_id,r.span_count,p.agent_uuid,p.metadata_json_v1 AS row_json,p.row_keys_json_v1 AS row_keys,p.metrics_json_v1 AS metrics,p.operation_previews_json_v1 AS operation_previews,p.source_revision`).WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "row_keys", "metrics", "operation_previews", "source_revision"}).AddRow("abc", 1, "agent", `{"trace_id":"abc","userId":null,"environment":null,"usage":{"total":9007199254740993}}`, `["trace_id","userId","usage","_metrics","_operation_previews","arbitrary.payload"]`, `{"observations":[{"sessionIds":["s1","s2"],"total":9007199254740993}]}`, `{"op":{"input":"preview","input_truncated":true}}`, 3))
	mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(windowSpanRows())
	mock.ExpectCommit()
	result, err := store.traceWindowRepresentation(context.Background(), "org", windowTestParams(), false, traceWindowPayloadLimit, true)
	require.NoError(t, err)
	row := result.(map[string]any)["rows"].([]map[string]any)[0]
	require.Contains(t, row, "userId")
	require.Nil(t, row["userId"])
	require.NotContains(t, row, "environment")
	require.NotContains(t, row, "arbitrary.payload")
	require.Equal(t, json.Number("9007199254740993"), row["usage"].(map[string]any)["total"])
	observation := row["_metrics"].(map[string]any)["observations"].([]any)[0].(map[string]any)
	require.Equal(t, []any{"s1", "s2"}, observation["sessionIds"])
	require.Equal(t, json.Number("9007199254740993"), observation["total"])
	require.Equal(t, map[string]any{"input": "preview", "input_truncated": true}, row["_operation_previews"].(map[string]any)["op"])
	require.EqualValues(t, 3, row["sourceRevision"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectMetricsWindowStoredColumns(mock sqlmock.Sqlmock) {
	for _, column := range [][2]string{{"metadata_json_v1", ingest.TraceProjectionMetadataExpression}, {"metrics_json_v1", "JSON_EXTRACT(row_json,'$._metrics')"}, {"row_keys_json_v1", "JSON_KEYS(row_json)"}, {"operation_previews_json_v1", ingest.TraceProjectionOperationPreviewsExpression}} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
	}
}

// Model only the SQL driver's stored-column boundary. Literal result assertions
// above remain independent of this fixture's document-to-column conversion.
func storedMetricsWindowRows(t *testing.T, document string) *sqlmock.Rows {
	t.Helper()
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(document), &row))
	keys := []string{}
	for key := range row {
		keys = append(keys, key)
	}
	encodedKeys, err := json.Marshal(keys)
	require.NoError(t, err)
	var metrics, previews any
	if value, ok := row["_metrics"]; ok {
		metrics = string(value)
	}
	if value, ok := row["_operation_previews"]; ok {
		previews = string(value)
	}
	return sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "row_keys", "metrics", "operation_previews", "source_revision"}).AddRow("abc", 1, "agent", document, string(encodedKeys), metrics, previews, 3)
}

func TestMetricsWindowUnverifiedStoredColumnsPreserveLegacyCompactFallback(t *testing.T) {
	for _, definition := range []string{"missing", "ordinary", "virtual", "conflicting"} {
		t.Run(definition, func(t *testing.T) {
			result := runMetricsWindowRequest(t, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectProjectionMaintenance(mock, true, false)
				mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
				rows := sqlmock.NewRows([]string{"kind", "extra", "expression"})
				switch definition {
				case "ordinary":
					rows.AddRow("longtext", "", ingest.TraceProjectionMetadataExpression)
				case "virtual":
					rows.AddRow("longtext", "VIRTUAL GENERATED", ingest.TraceProjectionMetadataExpression)
				case "conflicting":
					rows.AddRow("longtext", "STORED GENERATED", "'{}'")
				}
				mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(rows)
				mock.ExpectQuery(`SELECT COALESCE\(MAX\(OCTET_LENGTH\(p.row_json\)\),0\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"max_projection_bytes"}).AddRow(32 << 20))
				mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,p.row_json,p.source_revision").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "source_revision"}).AddRow("abc", 1, "agent", `{"trace_id":"abc","userId":null,"_metrics":{"observations":[]},"attributes.gen_ai.input.messages":"discard"}`, 3))
				mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(windowSpanRows())
				mock.ExpectCommit()
			})
			require.Len(t, result["rows"], 1)
			require.Empty(t, result["raw"])
			require.Equal(t, false, result["meta"].(map[string]any)["raw"])
			row := result["rows"].([]any)[0].(map[string]any)
			require.Contains(t, row, "userId")
			require.Nil(t, row["userId"])
			require.NotContains(t, row, "environment")
			require.NotContains(t, row, "attributes.gen_ai.input.messages")
			require.Equal(t, map[string]any{"observations": []any{}}, row["_metrics"])
		})
	}
}

func TestMetricsWindowRejectsInvalidStoredPresenceProof(t *testing.T) {
	for _, keys := range []any{nil, `null`, `{}`, `["trace_id",3]`, `["trace_id","trace_id"]`, `["trace_id"] {}`} {
		row, err := traceMetricsWindowStoredRow(map[string]any{"trace_id": "abc"}, map[string]any{"row_keys": keys})
		require.Error(t, err, "keys=%v", keys)
		require.Nil(t, row)
	}
	row, err := traceMetricsWindowStoredRow(map[string]any{"trace_id": "abc"}, map[string]any{"row_keys": `["trace_id","usage"]`})
	require.Error(t, err)
	require.Nil(t, row)
}
