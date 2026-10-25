package aivision

import (
	"context"
	"encoding/json"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func validProjection() traceProjection {
	return traceProjection{MaintenanceGeneration: 1, ProjectionContext: strings.Repeat("a", 64), SpaceID: "s", TraceID: "0123456789abcdef0123456789abcdef", SourceRevision: 1, SemanticsVersion: 1, AgentUUID: "a", Period: 100, Duration: nil, NodeCount: 1, Row: json.RawMessage(`{"trace_id":"0123456789abcdef0123456789abcdef","start_time_unix_nano":"100000000"}`), Preview: json.RawMessage(`{"version":"native-trace-preview-v1","complete":true,"input":"ok","output":"yes","inputTruncated":false,"outputTruncated":false,"rootObservationId":"root"}`)}
}
func TestProjectionRejectsMalformedAndUnsupportedContracts(t *testing.T) {
	p := validProjection()
	if err := validateTraceProjection(p); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*traceProjection){func(p *traceProjection) { p.SourceRevision = 0 }, func(p *traceProjection) { p.SemanticsVersion = 2 }, func(p *traceProjection) { p.Row = json.RawMessage(`[]`) }, func(p *traceProjection) { p.Row = json.RawMessage(`{"spans":[]}`) }, func(p *traceProjection) { p.NodeCount = -1 }, func(p *traceProjection) { p.SpaceID = "" }} {
		p := validProjection()
		mutate(&p)
		if validateTraceProjection(p) == nil {
			t.Fatalf("accepted malformed projection: %+v", p)
		}
	}
}
func TestProjectionQueryRejectsUnsupportedScopeExpansion(t *testing.T) {
	p := nativeListParams{ProjectionContext: strings.Repeat("a", 64), From: 1, To: 2, SpaceID: "s", AgentUUID: "a", Page: 1, Limit: 10}
	where, args, err := readModelWhere("org", p)
	if err != nil || where == "" || len(args) < 5 || args[0] != "org" || args[1] != "s" || args[2] != "a" {
		t.Fatalf("scope must lead predicates: %s %v %v", where, args, err)
	}
	p.SessionID = "unsupported-session"
	if _, _, err := readModelWhere("org", p); err == nil {
		t.Fatal("raw per-span identity filter must fall back")
	}
}

func TestPublishCompareAndSwapRejectsOldRevision(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := validProjection()
	p.UserID = "owner"
	p.AgentProduct = "product"
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", p.SpaceID, p.TraceID).WillReturnRows(sqlmock.NewRows([]string{"source_revision", "user_id", "agent_product", "scope_valid"}).AddRow(2, "owner", "product", true))
	mock.ExpectRollback()
	result, err := s.publishTraceProjection(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("source_revision_changed"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestPublishCannotChangePhysicalAuthorizationScope(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := validProjection()
	p.UserID = "attacker"
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", p.SpaceID, p.TraceID).WillReturnRows(sqlmock.NewRows([]string{"source_revision", "user_id", "agent_product", "scope_valid"}).AddRow(1, "owner", "product", true))
	mock.ExpectRollback()
	result, err := s.publishTraceProjection(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("source_scope_unprojectable"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestPageRequiresCompleteHistoricalCoverageBeforeEmptySuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "s").WillReturnRows(sqlmock.NewRows([]string{"complete"}))
	mock.ExpectRollback()
	p := nativeListParams{ProjectionContext: strings.Repeat("a", 64), SpaceID: "s", AgentUUID: "agent", From: 1, To: 2, Page: 1, Limit: 10}
	result, err := s.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("historical_coverage_incomplete"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestPageRejectsStaleAndBoundarySources(t *testing.T) {
	for _, tc := range []struct {
		name, reason    string
		stale, boundary int
	}{{"stale", "projection_coverage_stale", 1, 0}, {"boundary", "boundary_crossing_trace", 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "s").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
			mock.ExpectQuery("SELECT COALESCE\\(SUM").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary"}).AddRow(tc.stale, tc.boundary))
			mock.ExpectRollback()
			result, err := s.readModelPage(context.Background(), "org", nativeListParams{ProjectionContext: strings.Repeat("a", 64), SpaceID: "s", AgentUUID: "agent", From: 1, To: 2, Page: 1, Limit: 10})
			require.NoError(t, err)
			require.Equal(t, readModelUnsupported(tc.reason), result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
func TestSourceSnapshotCountAndSpansUseOneTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid").WithArgs("org", "space", "abc").WillReturnRows(sqlmock.NewRows([]string{"revision", "min", "max", "user_id", "product", "valid"}).AddRow(4, 1000000000, 1000000000, "owner", "prod", true))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\),COALESCE").WithArgs("org", "space", "abc").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max_bytes"}).AddRow(1, 3000, 3000))
	mock.ExpectQuery("SELECT trace_id,").WithArgs("org", "space", "abc").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_id", "start_time_unix_nano", "end_time_unix_nano", "status_code", "resource_attributes", "attributes", "events", "links", "payload", "user_id"}).AddRow("abc", "def", "1000000000", "1000000001", 0, `{"aivision.agent.id":"agent"}`, `{}`, `[]`, `[]`, `{}`, "owner"))
	mock.ExpectCommit()
	result, err := s.traceSourceSnapshot(context.Background(), "org", "space", "abc")
	require.NoError(t, err)
	response := result.(map[string]any)
	require.EqualValues(t, 1, response["span_count"])
	require.Equal(t, true, response["complete"])
	require.EqualValues(t, 4, response["source_revision"])
	require.Equal(t, "owner", response["source_user_id"])
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestReadModelLiteralTagsAndNanosecondOrder(t *testing.T) {
	p := nativeListParams{ProjectionContext: strings.Repeat("a", 64), SpaceID: "s", AgentUUID: "agent", User: "owner", From: 1, To: 2, Page: 1, Limit: 10, Tags: []string{"a", "b"}, Keyword: "%_\\中文", SearchType: "input"}
	where, args, err := readModelWhere("org", p)
	require.NoError(t, err)
	require.Contains(t, where, " OR JSON_CONTAINS")
	require.Contains(t, where, "LOCATE(BINARY ?,BINARY p.input_text)")
	require.Contains(t, args, "%_\\中文")
	require.Contains(t, args, "owner")
	order, err := readModelOrder("start_time_unix_nano.desc")
	require.NoError(t, err)
	require.Contains(t, order, "p.start_ns DESC")
	require.Contains(t, order, "p.trace_id DESC")
	p.Filters = []nativeFilter{{Field: "span_count", Op: ">=", Value: json.Number("9007199254740993")}}
	_, _, err = readModelWhere("org", p)
	require.Error(t, err)
}
func TestReadModelRequiresTransactionalStore(t *testing.T) {
	require.Equal(t, "transactional_store_unavailable", (&oceanBaseStore{}).readModelUnavailable())
}

func TestPreviewPreservesNullAndEmptyIO(t *testing.T) {
	p := validProjection()
	p.Preview = json.RawMessage(`{"version":"native-trace-preview-v1","complete":true,"input":null,"output":"","inputTruncated":false,"outputTruncated":false,"rootObservationId":"root"}`)
	require.NoError(t, validateTraceProjection(p))
	p.Preview = json.RawMessage(`{"version":"native-trace-preview-v1","complete":true,"output":"","inputTruncated":false,"outputTruncated":false,"rootObservationId":"root"}`)
	require.Error(t, validateTraceProjection(p))
}

func TestIndependentFacetWindowPreservesAuthorizationScope(t *testing.T) {
	p := nativeListParams{ProjectionContext: strings.Repeat("a", 64), SpaceID: "space", User: "physical-owner", AgentProduct: "product", AgentUUID: "agent", From: 100, To: 200, Page: 2, Limit: 1, Keyword: "input", InputKeyword: "body", Tags: []string{"tag"}, Env: "prod", Scene: "test", Username: "sdk-user", TraceID: "abc", Filters: []nativeFilter{{Field: "duration_ms", Op: ">", Value: 1}}}
	window, filtered := readModelWindowParams(p)
	require.True(t, filtered)
	require.Equal(t, "space", window.SpaceID)
	require.Equal(t, "physical-owner", window.User)
	require.Equal(t, "product", window.AgentProduct)
	require.Equal(t, "agent", window.AgentUUID)
	require.Equal(t, p.From, window.From)
	require.Equal(t, p.To, window.To)
	require.Equal(t, p.ProjectionContext, window.ProjectionContext)
	_, stillFiltered := readModelWindowParams(window)
	require.False(t, stillFiltered)
}

func TestPublishRejectsGenerationChangeBeforeReusedRevision(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := validProjection()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 2, 1))
	mock.ExpectRollback()
	result, err := s.publishTraceProjection(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("maintenance_generation_changed"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSourceSnapshotRejectsStatusPayloadBeforeFetchingLongtext(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT source_revision,min_timestamp").WillReturnRows(sqlmock.NewRows([]string{"revision", "min", "max", "user", "product", "valid"}).AddRow(1, 100, 200, "owner", "product", true))
	mock.ExpectQuery("SELECT COUNT.*OCTET_LENGTH.*status_message").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max_bytes"}).AddRow(5000, 300*1024*1024, 60000))
	mock.ExpectRollback()
	result, err := s.traceSourceSnapshot(context.Background(), "org", "space", "abc")
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("source_snapshot_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
