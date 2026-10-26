package aivision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

func expectProjectionMaintenance(mock sqlmock.Sqlmock, enabled bool, lock bool) {
	query := "SELECT enabled,generation,schema_version FROM"
	if lock {
		query = "SELECT enabled,generation,schema_version FROM.*FOR UPDATE"
	}
	mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(enabled, 1, ingest.ProjectionSchemaVersion))
}

func expectWindowSummaryBytes(mock sqlmock.Sqlmock, count int64) {
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(OCTET_LENGTH\\(p.row_json\\)").WillReturnRows(sqlmock.NewRows([]string{"summary_bytes"}).AddRow(count))
}

func windowTestParams() nativeListParams {
	return nativeListParams{From: 100, To: 200, SpaceID: "space", User: "owner", AgentProduct: "product", AgentUUID: "agent", ProjectionContext: strings.Repeat("a", 64)}
}
func windowSpanRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"trace_id", "span_id", "resource_attributes", "attributes", "events", "links", "payload", "start_time_unix_nano", "end_time_unix_nano", "status_code"})
}
func addWindowSpan(rows *sqlmock.Rows, id string) *sqlmock.Rows {
	return rows.AddRow(id, "span", `{"aivision.agent.id":"agent"}`, `{"count":9007199254740993}`, `[]`, `[]`, `{}`, "100000000", "100000001", 0)
}

func TestRecoveryWindowRawWorksWithoutProjectionTables(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT BINARY trace_id\\)").WithArgs("org", "org", "space", "space", "owner", "owner", "product", "product", int64(100000000), int64(200000000)).WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(windowSpanRows(), "abc"))
	mock.ExpectCommit()
	result, e := s.traceWindow(context.Background(), "org", windowTestParams(), true)
	require.NoError(t, e)
	r := result.(map[string]any)
	require.True(t, r["complete"].(bool))
	require.Equal(t, traceWindowContract, r["contract"])
	require.NotContains(t, r["meta"].(map[string]any), "maintenance_generation")
	raw := r["raw"].([]map[string]any)
	span := raw[0]["spans"].([]map[string]any)[0]
	require.Equal(t, json.Number("9007199254740993"), span["attributes"].(map[string]any)["count"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowRawAcceptsMoreThanOld10000TraceLimit(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(10001, 10001, 30000000, 3000))
	rows := windowSpanRows()
	for i := 0; i < 10001; i++ {
		addWindowSpan(rows, fmt.Sprintf("%032x", i))
	}
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(rows)
	mock.ExpectCommit()
	result, e := s.traceWindow(context.Background(), "org", windowTestParams(), true)
	require.NoError(t, e)
	require.Len(t, result.(map[string]any)["raw"], 10001)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowMaintenanceOffUsesRawOnly(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, false, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(windowSpanRows(), "abc"))
	mock.ExpectCommit()
	result, e := s.traceWindow(context.Background(), "org", windowTestParams(), false)
	require.NoError(t, e)
	require.True(t, result.(map[string]any)["meta"].(map[string]any)["raw"].(bool))
	require.NotContains(t, result.(map[string]any)["meta"].(map[string]any), "maintenance_generation")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowMergesCleanAndDirtyInOneSnapshot(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
	expectWindowSummaryBytes(mock, 0)
	mock.ExpectQuery("SELECT r.trace_id,r.span_count,p.agent_uuid,p.row_json").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "preview_json", "source_revision"}).AddRow("abc", 1, "agent", `{"trace_id":"abc","attributes.gen_ai.input.messages":"complete searchable input"}`, `{"input":"bounded"}`, 3))
	mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(addWindowSpan(windowSpanRows(), "def"))
	mock.ExpectCommit()
	result, e := s.traceWindow(context.Background(), "org", windowTestParams(), false)
	require.NoError(t, e)
	r := result.(map[string]any)
	require.Len(t, r["rows"], 1)
	require.Len(t, r["raw"], 1)
	require.EqualValues(t, 1, r["meta"].(map[string]any)["maintenance_generation"])
	require.Equal(t, "complete searchable input", r["rows"].([]map[string]any)[0]["attributes.gen_ai.input.messages"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowRejectsBoundsAndIncompleteWithoutSuccess(t *testing.T) {
	for _, tc := range []struct {
		name                string
		count, spans, bytes int64
		fetch               bool
	}{{"too_many_traces", 50001, 50001, 100, false}, {"too_many_spans", 1, 500001, 100, false}, {"too_many_bytes", 1, 1, traceWindowSourceByteLimit + 1, false}, {"incomplete", 1, 1, 100, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(tc.count, tc.spans, tc.bytes, 3000))
			if tc.fetch {
				mock.ExpectQuery("SELECT trace_id,").WillReturnRows(windowSpanRows())
			}
			mock.ExpectRollback()
			r, e := s.traceWindow(context.Background(), "org", windowTestParams(), true)
			require.Error(t, e)
			require.Nil(t, r)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRecoveryWindowScopeUsesExactAuthorizationValues(t *testing.T) {
	p := windowTestParams()
	p.User = " owner "
	q, args, e := traceWindowScope("org", p, "r.")
	require.NoError(t, e)
	require.Contains(t, q, "BINARY r.user_id=BINARY ?")
	require.Contains(t, q, "BINARY r.space_id=BINARY ?")
	require.Equal(t, " owner ", args[4])
	require.Contains(t, q, "r.timestamp>=? AND r.timestamp<?")
	relation, values := (&oceanBaseStore{cfg: config{TablePrefix: "test"}}).windowCleanRelation("org", p)
	require.Contains(t, relation, "s.dirty=FALSE")
	require.Contains(t, relation, "p.source_revision=s.source_revision")
	require.Contains(t, relation, "s.min_timestamp>=? AND s.max_timestamp<?")
	require.Equal(t, " owner ", values[7])
}

func TestRecoveryWindowParserRejectsExpansionAndRepeatedControls(t *testing.T) {
	h := &handler{cfg: config{MaxQueryRange: 31 * 24 * 60 * 60 * 1000000000}}
	base := "/?from=100&to=200&space_id=space&agent_uuid=agent&projection_context=" + strings.Repeat("a", 64) + "&raw=false"
	_, raw, e := h.traceWindowParams(httptest.NewRequest("GET", base, nil))
	require.NoError(t, e)
	require.False(t, raw)
	_, _, e = h.traceWindowParams(httptest.NewRequest("GET", strings.Replace(base, "&agent_uuid=agent", "", 1), nil))
	require.NoError(t, e)

	physical, _, e := h.traceWindowParams(httptest.NewRequest("GET", base+"&user=%20owner%20&agent_product=%20product%20", nil))
	require.NoError(t, e)
	require.Equal(t, " owner ", physical.User)
	require.Equal(t, " product ", physical.AgentProduct)

	for _, suffix := range []string{"&raw=true", "&username=x", "&projection_context=bad", "&agent_uuid=other"} {
		_, _, e := h.traceWindowParams(httptest.NewRequest("GET", base+suffix, nil))
		require.Error(t, e)
	}
}

func TestRecoveryWindowValidatesEachProjectionBeforeReadingNext(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
	rows := sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "preview_json", "source_revision"}).AddRow("abc", 1, "agent", "{invalid", `{}`, 1).AddRow("def", 1, "agent", `{}`, `{}`, 1).RowError(1, fmt.Errorf("later row must not be materialized"))
	expectWindowSummaryBytes(mock, 0)
	mock.ExpectQuery("SELECT r.trace_id,r.span_count").WillReturnRows(rows)
	mock.ExpectRollback()
	result, e := s.traceWindow(context.Background(), "org", windowTestParams(), false)
	require.ErrorContains(t, e, "invalid canonical recovery row")
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

// An available summary must not make a complete raw window unreadable merely
// because its denormalized canonical JSON is larger than the raw representation.
func TestRecoveryWindowOversizedCanonicalRowsFallsBackToCompleteRawSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
	expectWindowSummaryBytes(mock, 0)
	mock.ExpectQuery("SELECT r.trace_id,r.span_count").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "preview_json", "source_revision"}).AddRow("abc", 1, "agent", `{"trace_id":"abc","input":"`+strings.Repeat("x", 8192)+`"}`, `{}`, 1))
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(addWindowSpan(windowSpanRows(), "abc"), "def"))
	mock.ExpectCommit()
	result, err := store.traceWindowWithPayloadLimit(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+4096)
	require.NoError(t, err)
	data := result.(map[string]any)
	require.Empty(t, data["rows"])
	require.Len(t, data["raw"], 2)
	meta := data["meta"].(map[string]any)
	require.True(t, meta["raw"].(bool))
	require.EqualValues(t, 0, meta["projectedTraceCount"])
	require.EqualValues(t, 0, meta["omittedAgentTraceCount"])
	require.EqualValues(t, 2, meta["rawTraceCount"])
	require.EqualValues(t, 2, meta["rawSpanCount"])
	require.Equal(t, "canonical_summary_budget", meta["recoveryReason"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowOversizedSummaryPreflightAvoidsLoadingCanonicalRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
	mock.ExpectQuery("SELECT COALESCE\\(SUM\\(OCTET_LENGTH\\(p.row_json\\)").WillReturnRows(sqlmock.NewRows([]string{"summary_bytes"}).AddRow(8192))
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(addWindowSpan(windowSpanRows(), "abc"), "def"))
	mock.ExpectCommit()
	result, err := store.traceWindowWithPayloadLimit(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+4096)
	require.NoError(t, err)
	data := result.(map[string]any)
	require.Empty(t, data["rows"])
	require.Len(t, data["raw"], 2)
	meta := data["meta"].(map[string]any)
	require.True(t, meta["raw"].(bool))
	require.EqualValues(t, 0, meta["projectedTraceCount"])
	require.Equal(t, "canonical_summary_budget", meta["recoveryReason"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowCombinedBudgetRetriesCompleteRawInSameSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(2, 2, 6000, 3000))
	expectWindowSummaryBytes(mock, 0)
	mock.ExpectQuery("SELECT r.trace_id,r.span_count").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "agent_uuid", "row_json", "preview_json", "source_revision"}).AddRow("abc", 1, "agent", `{"trace_id":"abc","input":"`+strings.Repeat("y", 3000)+`"}`, `{}`, 1))
	mock.ExpectQuery("SELECT trace_id,.*NOT EXISTS").WillReturnRows(addWindowSpan(windowSpanRows(), "def"))
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(addWindowSpan(windowSpanRows(), "abc"), "def"))
	mock.ExpectCommit()
	result, err := store.traceWindowWithPayloadLimit(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+4096)
	require.NoError(t, err)
	data := result.(map[string]any)
	require.Empty(t, data["rows"])
	require.Len(t, data["raw"], 2)
	meta := data["meta"].(map[string]any)
	require.True(t, meta["raw"].(bool))
	require.EqualValues(t, 0, meta["projectedTraceCount"])
	require.EqualValues(t, 2, meta["rawSpanCount"])
	require.Equal(t, "canonical_summary_budget", meta["recoveryReason"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveryWindowProjectionErrorsDoNotBecomeRawSuccess(t *testing.T) {
	for _, stage := range []string{"preflight_sql", "canonical_sql_with_budget_message"} {
		t.Run(stage, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			failure := errors.New(errTraceWindowPayloadBudget.Error())
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
			if stage == "preflight_sql" {
				mock.ExpectQuery("SELECT COALESCE").WillReturnError(failure)
			} else {
				expectWindowSummaryBytes(mock, 0)
				mock.ExpectQuery("SELECT r.trace_id,r.span_count").WillReturnError(failure)
			}
			mock.ExpectRollback()
			result, err := store.traceWindowWithPayloadLimit(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+4096)
			require.ErrorIs(t, err, failure)
			require.Nil(t, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestRecoveryWindowRawBudgetStillFailsAfterSummaryFallback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"traces", "spans", "bytes", "max_bytes"}).AddRow(1, 1, 3000, 3000))
	expectWindowSummaryBytes(mock, 8192)
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(addWindowSpan(windowSpanRows(), "abc"))
	mock.ExpectRollback()
	result, err := store.traceWindowWithPayloadLimit(context.Background(), "org", windowTestParams(), false, traceWindowEnvelopeReserve+128)
	require.ErrorIs(t, err, errTraceWindowPayloadBudget)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}
