package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

func TestOperationFactsSelectionDoesNotReadRawProjection(t *testing.T) {
	query := operationFactsColumnsSQL()
	require.NotContains(t, query, "row_json", "select scalar operation keys/facts before loading any preview source")
	require.NotContains(t, query, "JSON_EXTRACT", "selected previews are loaded once per Trace after a stored-byte preflight")
}

func expectOperationPreviewCapability(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionOperationPreviewsColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", ingest.TraceProjectionOperationPreviewsExpression))
}

func TestOperationPageLoadsSelectedTracePreviewMapOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := windowTestParams()
	p.OperationFacts = true
	expectOperationReadGate(mock, 0, 2)
	expectOperationPreviewCapability(mock)
	columns := []string{"trace_id", "observation_id", "type", "start_ns", "source_revision", "span_count", "model", "provider", "ambiguous", "input_tokens", "output_tokens", "total_tokens", "cache_read_input_tokens", "cache_write_input_tokens", "reasoning_tokens"}
	keys := sqlmock.NewRows(columns)
	for i, id := range []string{"f", "e", "d"} {
		trace := "a"
		if i == 2 {
			trace = "b"
		}
		keys.AddRow(trace, id, "GENERATION", 150000003-i, 7, 3, "model", "provider", false, "9007199254740993", "7", "9007199254741000", "0", nil, nil)
	}
	mock.ExpectQuery("SELECT t.trace_id,c.observation_id.*ORDER BY").WillReturnRows(keys)
	preview := map[string]any{"name": "LLM", "latency": 1, "status": 1, "input": "complete selected input", "output": "complete selected output", "input_truncated": false, "output_truncated": true}
	encoded, err := json.Marshal(map[string]any{"f": preview, "e": preview, "unselected": map[string]any{"input": strings.Repeat("large unused preview ", 10000)}})
	require.NoError(t, err)
	mock.ExpectQuery(`SELECT p.trace_id,p.source_revision,OCTET_LENGTH\(p.operation_previews_json_v1\)`).WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "preview_bytes"}).AddRow("a", 7, len(encoded)))
	mock.ExpectQuery(`SELECT p.operation_previews_json_v1 FROM`).WillReturnRows(sqlmock.NewRows([]string{"previews"}).AddRow(string(encoded)))
	mock.ExpectCommit()
	result, err := store.traceOperationPage(context.Background(), "org", p, 2)
	require.NoError(t, err)
	page := result.(map[string]any)
	require.Equal(t, true, page["has_more"])
	data := page["data"].([]map[string]any)
	require.Len(t, data, 2)
	for _, item := range data {
		require.Equal(t, "9007199254740993", item["facts"].(map[string]any)["input"])
		require.Equal(t, "complete selected input", item["preview"].(map[string]any)["input"])
	}
	require.NoError(t, mock.ExpectationsWereMet(), "two operations from one Trace use one stored map load; look-ahead Trace is not loaded")
}

func expectOperationPreviewMap(mock sqlmock.Sqlmock, preview any) {
	value := "null"
	if text, ok := preview.(string); ok {
		value = text
	}
	document := `{"f":` + value + `}`
	mock.ExpectQuery(`SELECT p.trace_id,p.source_revision,OCTET_LENGTH\(p.operation_previews_json_v1\)`).WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "preview_bytes"}).AddRow("a", 7, len(document)))
	mock.ExpectQuery(`SELECT p.operation_previews_json_v1 FROM`).WillReturnRows(sqlmock.NewRows([]string{"previews"}).AddRow(document))
}

func TestOperationPageRejectsOversizePreviewSourceBeforeLoading(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := windowTestParams()
	p.OperationFacts = true
	expectOperationReadGate(mock, 0, 1)
	expectOperationPreviewCapability(mock)
	mock.ExpectQuery("SELECT t.trace_id,c.observation_id").WillReturnRows(operationFactRows(nil))
	mock.ExpectQuery(`SELECT p.trace_id,p.source_revision,OCTET_LENGTH\(p.operation_previews_json_v1\)`).WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "preview_bytes"}).AddRow("a", 7, sourcePageTraceByteLimit+1))
	mock.ExpectRollback()
	result, err := store.traceOperationPage(context.Background(), "org", p, 1)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("operation_page_source_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSelectedOperationPreviewMapPreservesOnlyWantedValues(t *testing.T) {
	preview := `{"name":"complete","latency":1,"status":1,"input":"full tail","output":null,"input_truncated":false,"output_truncated":false}`
	source := `{"unused":{"input":"` + strings.Repeat("unselected ", 100000) + `"},"wanted":` + preview + `}`
	selected, err := selectedOperationPreviews(source, map[string]bool{"wanted": true})
	require.NoError(t, err)
	require.Len(t, selected, 1)
	require.Equal(t, "full tail", selected["wanted"]["input"])
	_, err = selectedOperationPreviews(source, map[string]bool{"missing": true})
	require.Error(t, err)
}

func TestOceanBaseOperationPreviewSourcesKeepLargeUnrelatedIOOutOfQueries(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated SQL DSN")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	prefix := fmt.Sprintf("operation_%x", time.Now().UnixNano())
	table := "`" + prefix + "_trace_projections`"
	defer func() { _, e := db.Exec("DROP TABLE IF EXISTS " + table); require.NoError(t, e) }()
	_, err = db.Exec("CREATE TABLE " + table + " (org_id VARCHAR(255),space_id VARCHAR(255),trace_id VARCHAR(32),source_revision BIGINT,projection_context CHAR(64),row_json LONGTEXT," + ingest.TraceProjectionOperationPreviewsColumn + " LONGTEXT GENERATED ALWAYS AS (" + ingest.TraceProjectionOperationPreviewsExpression + ") STORED,PRIMARY KEY(org_id,space_id,trace_id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	require.NoError(t, err)
	preview := map[string]any{"name": "operation", "latency": 1, "status": 1, "input": "complete tail", "output": nil, "input_truncated": false, "output_truncated": false}
	previews, err := json.Marshal(map[string]any{"f": preview, "e": preview})
	require.NoError(t, err)
	p := windowTestParams()
	// Keep each source below the engine's default 16 MiB packet limit while
	// proving that selected previews do not load its unrelated multi-MiB IO.
	_, err = db.Exec("INSERT INTO "+table+" (org_id,space_id,trace_id,source_revision,projection_context,row_json) VALUES (?,?,?,?,?,JSON_OBJECT('attributes.gen_ai.input.messages',REPEAT('x',?), '_operation_previews',JSON_EXTRACT(?,'$')))", "org", p.SpaceID, "a", 7, p.ProjectionContext, 8<<20, string(previews))
	require.NoError(t, err)
	var sourceBytes int64
	require.NoError(t, db.QueryRow("SELECT OCTET_LENGTH(row_json) FROM "+table+" WHERE trace_id='a'").Scan(&sourceBytes))
	require.Greater(t, sourceBytes, int64(8<<20))
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: prefix}}
	tx, err := store.beginReadModel(context.Background(), true)
	require.NoError(t, err)
	defer tx.Rollback()
	ready, err := ingest.TraceProjectionOperationPreviewsReady(context.Background(), tx, prefix)
	require.NoError(t, err)
	require.True(t, ready)
	rows := []map[string]any{{"trace_id": "a", "observation_id": "f", "source_revision": int64(7)}, {"trace_id": "a", "observation_id": "e", "source_revision": int64(7)}}
	selected, reason, err := store.operationPagePreviews(context.Background(), tx, "org", p, rows)
	require.NoError(t, err)
	require.Empty(t, reason)
	require.Len(t, selected["a"], 2)
	require.Equal(t, "complete tail", selected["a"]["f"]["input"])
	require.NoError(t, tx.Commit())
}
