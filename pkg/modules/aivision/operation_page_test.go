package aivision

import (
	"context"
	"database/sql/driver"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func expectOperationReadGate(mock sqlmock.Sqlmock, incomplete, selected int64, generations ...int64) {
	mock.ExpectBegin()
	generation := int64(1)
	if len(generations) > 0 {
		generation = generations[0]
	}
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "version"}).AddRow(true, generation, 1))
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
	expectStatisticsReady(mock)
	var latency any
	if selected > 0 {
		latency = float64(selected) * 2
	}
	mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(2, 6, incomplete, selected, selected*3, selected, selected, 0, selected, latency))
}
func operationRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"trace_id", "observation_id", "type", "start_ns", "source_revision", "span_count"})
}

func TestOperationPageRejectsUnknownRepeatedAndInvalidLimit(t *testing.T) {
	h := &handler{}
	for _, suffix := range []string{"&limit=0", "&limit=201", "&limit=1&limit=2", "&limit=1&raw=true", "&limit=1&filters=[]"} {
		request := httptest.NewRequest("GET", "/?from=100&to=200&space_id=s&projection_context="+strings.Repeat("a", 64)+suffix, nil)
		_, _, e := h.operationPageParams(request)
		require.Error(t, e)
	}
}

func TestOperationPageAcceptsExplicitBoundedFacts(t *testing.T) {
	h := &handler{cfg: config{MaxQueryRange: time.Hour}}
	r := httptest.NewRequest("GET", "/?from=100&to=200&space_id=s&projection_context="+strings.Repeat("a", 64)+"&include_facts=true", nil)
	_, _, err := h.operationPageParams(r)
	require.NoError(t, err)
}

func operationFactRows(preview any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"trace_id", "observation_id", "type", "start_ns", "source_revision", "span_count", "model", "provider", "ambiguous", "input_tokens", "output_tokens", "total_tokens", "cache_read_input_tokens", "cache_write_input_tokens", "reasoning_tokens"}).AddRow("a", "f", "GENERATION", "150000001", 7, 3, "model", "provider", false, "9007199254740993", "7", "9007199254741000", "0", nil, nil)
}

func TestOperationPageFactsPreserveExactTokensAndBoundedPreview(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := windowTestParams()
	p.OperationFacts = true
	expectOperationReadGate(mock, 0, 1)
	expectOperationPreviewCapability(mock)
	mock.ExpectQuery(`SELECT t.trace_id,c.observation_id`).WillReturnRows(operationFactRows(`{"name":"LLM","latency":12.5,"status":1,"input":"\"question\"","output":"\"answer\"","input_truncated":false,"output_truncated":true}`))
	expectOperationPreviewMap(mock, `{"name":"LLM","latency":12.5,"status":1,"input":"\"question\"","output":"\"answer\"","input_truncated":false,"output_truncated":true}`)
	mock.ExpectCommit()
	result, err := s.traceOperationPage(context.Background(), "org", p, 1)
	require.NoError(t, err)
	item := result.(map[string]any)["data"].([]map[string]any)[0]
	require.Equal(t, "9007199254740993", item["facts"].(map[string]any)["input"])
	require.Equal(t, true, item["preview"].(map[string]any)["output_truncated"])
	require.Equal(t, `"question"`, item["preview"].(map[string]any)["input"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageFactsDeclineMissingOrOversizePreviews(t *testing.T) {
	for _, preview := range []any{nil, `{"name":"LLM","latency":1,"status":1,"input":"` + strings.Repeat("a", 1025) + `","output":null,"input_truncated":true,"output_truncated":false}`} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
		p := windowTestParams()
		p.OperationFacts = true
		expectOperationReadGate(mock, 0, 1)
		expectOperationPreviewCapability(mock)
		mock.ExpectQuery(`SELECT t.trace_id,c.observation_id`).WillReturnRows(operationFactRows(preview))
		expectOperationPreviewMap(mock, preview)
		mock.ExpectRollback()
		result, err := s.traceOperationPage(context.Background(), "org", p, 1)
		require.NoError(t, err)
		require.Equal(t, readModelUnsupported("selected_operation_preview_unavailable"), result)
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestOperationPageKeepsUnselectedAgentCompletenessGate(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectOperationReadGate(mock, 1, 0)
	mock.ExpectRollback()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 10)
	require.NoError(t, e)
	require.Equal(t, map[string]any{"supported": false, "reason": "statistics_window_incomplete"}, value)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageReturnsOnlyLatestRevisionBoundKeys(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectOperationReadGate(mock, 0, 1)
	_, _, args, e := store.statisticsSelectedRelations("org", windowTestParams())
	require.NoError(t, e)
	wire := []driver.Value{}
	for _, value := range args {
		wire = append(wire, value)
	}
	wire = append(wire, int64(2))
	mock.ExpectQuery("SELECT .*ORDER BY c.start_ns DESC,BINARY t.trace_id DESC,BINARY c.observation_id DESC LIMIT \\?").WithArgs(wire...).WillReturnRows(operationRows().AddRow("a", "f", "GENERATION", "150000001", 7, 3).AddRow("a", "e", "TOOL", "150000000", 7, 3))
	mock.ExpectCommit()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 1)
	require.NoError(t, e)
	result := value.(map[string]any)
	require.Equal(t, "native-trace-operation-page-v1", result["contract"])
	require.Equal(t, true, result["complete"])
	require.Equal(t, true, result["has_more"])
	data := result["data"].([]map[string]any)
	require.Len(t, data, 1)
	require.Equal(t, map[string]any{"trace_id": "a", "observation_id": "f", "type": "GENERATION", "start_ns": "150000001", "source_revision": int64(7), "span_count": int64(3)}, data[0])
	require.EqualValues(t, 1, result["meta"].(map[string]any)["limit"])
	require.EqualValues(t, 1, result["meta"].(map[string]any)["maintenance_generation"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageBindsCurrentMaintenanceGeneration(t *testing.T) {
	for _, generation := range []int64{1, 2} {
		db, mock, e := sqlmock.New()
		require.NoError(t, e)
		store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
		expectOperationReadGate(mock, 0, 0, generation)
		mock.ExpectCommit()
		value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 200)
		require.NoError(t, e)
		require.Equal(t, generation, value.(map[string]any)["meta"].(map[string]any)["maintenance_generation"])
		require.NoError(t, mock.ExpectationsWereMet())
		_ = db.Close()
	}
}

func TestOperationPageRejectsNonpositiveMaintenanceGeneration(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "version"}).AddRow(true, 0, 1))
	mock.ExpectRollback()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 200)
	require.ErrorContains(t, e, "generation")
	require.Nil(t, value)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageEmergencyOffNeverReadsDerivedKeys(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, false, false)
	mock.ExpectRollback()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 200)
	require.NoError(t, e)
	require.Equal(t, map[string]any{"supported": false, "reason": "projection_maintenance_unavailable"}, value)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageEmptySelectionSkipsKeyRead(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectOperationReadGate(mock, 0, 0)
	mock.ExpectCommit()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 10)
	require.NoError(t, e)
	result := value.(map[string]any)
	require.Empty(t, result["data"])
	require.Equal(t, false, result["has_more"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestOperationPageNeverAcceptsDuplicateOrOutOfOrderKeys(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectOperationReadGate(mock, 0, 1)
	mock.ExpectQuery("SELECT .*ORDER BY c.start_ns DESC").WillReturnRows(operationRows().AddRow("a", "f", "GENERATION", "150000001", 7, 3).AddRow("a", "f", "GENERATION", "150000001", 7, 3))
	mock.ExpectRollback()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 2)
	require.Error(t, e)
	require.Nil(t, value)
	require.NoError(t, mock.ExpectationsWereMet())
}
