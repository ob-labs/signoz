package aivision

import (
	"context"
	"database/sql/driver"
	"net/http/httptest"
	"strings"
	"testing"

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
