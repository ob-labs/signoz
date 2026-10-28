package aivision

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/stretchr/testify/require"
)

func sessionReadModelTestParams() sessionReadModelParams {
	return sessionReadModelParams{nativeListParams: nativeListParams{ProjectionContext: strings.Repeat("a", 64), SpaceID: "space", AgentUUID: "agent", User: "owner", AgentProduct: "product", From: 100, To: 200, Page: 1, Limit: 20}}
}

func TestSessionReadModelPredicatesPreserveCompleteGroup(t *testing.T) {
	p := sessionReadModelTestParams()
	p.Username = "sdk-user"
	p.Tags = []string{"skill:one", "mcp:two"}
	p.Keyword = "%_中文"
	p.SearchType = "input"
	p.SessionKeyword = "needle"
	p.SessionStatus = "error"
	count := int64(2)
	tokens := int64(0)
	duration := 60000.0
	p.MinTraceCount, p.MinTotalTokens, p.MinDurationMS = &count, &tokens, &duration
	query, args, err := sessionGroupSQL("org", "`projections`", p, "")
	require.NoError(t, err)
	require.Contains(t, query, "CASE WHEN")
	require.Contains(t, query, "AS matched")
	require.Contains(t, query, "COUNT(*) AS trace_count")
	require.Contains(t, query, "HAVING SUM(matched)>0")
	require.Contains(t, query, "COUNT(trace_tokens)=COUNT(*)")
	require.Contains(t, query, "COUNT(end_ns)=COUNT(*)")
	require.Contains(t, query, "LOCATE(BINARY ?,BINARY p.input_text)")
	require.Contains(t, query, " OR JSON_CONTAINS")
	require.Contains(t, args, "%_中文")
	require.Contains(t, args, "owner")
	query, args, err = sessionGroupSQL("org", "`projections`", p, "username")
	require.NoError(t, err)
	require.NotContains(t, query, "BINARY p.username=BINARY ?")
	require.NotContains(t, args, "sdk-user")
}

func TestSessionReadModelParserRejectsSilentUnsupportedControls(t *testing.T) {
	h := &handler{cfg: config{MaxLimit: 10000, MaxQueryRange: time.Hour}}
	base := "/?space_id=space&agent_uuid=agent&projection_context=" + strings.Repeat("a", 64) + "&from=100&to=200&limit=20"
	p, err := h.sessionReadModelParams(httptest.NewRequest("GET", base+"&min_total_tokens=0&min_trace_count=2&min_duration_ms=60000&tags=%5B%22x%22%5D", nil))
	require.NoError(t, err)
	require.EqualValues(t, 0, *p.MinTotalTokens)
	require.Equal(t, []string{"x"}, p.Tags)
	for _, suffix := range []string{"&min_total_tokens=-1", "&min_trace_count=0", "&min_duration_ms=NaN", "&session_status=bogus", "&filters=%5B%5D", "&limit=51", "&keyword=x&searchType=traceId"} {
		_, err := h.sessionReadModelParams(httptest.NewRequest("GET", base+suffix, nil))
		require.Error(t, err, suffix)
	}
}

func TestSessionReadModelRequiresHistoricalCoverage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "space").WillReturnRows(sqlmock.NewRows([]string{"complete"}))
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("historical_coverage_incomplete"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSessionReadModelRejectsOversizedGroupWithoutTruncation(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT session_id,COUNT").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("large-session", sessionPageTraceLimit+1, 123))
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("session_page_trace_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSessionReadModelPreservesLiteralSDKDimensions(t *testing.T) {
	h := &handler{cfg: config{MaxLimit: 10000, MaxQueryRange: time.Hour}}
	p, err := h.sessionReadModelParams(httptest.NewRequest("GET", "/?space_id=space&agent_uuid=agent&projection_context="+strings.Repeat("a", 64)+"&from=100&to=200&username=%20sdk%20&keyword=%20literal%20&searchType=input&session_id=%20id%20", nil))
	require.NoError(t, err)
	require.Equal(t, " sdk ", p.Username)
	require.Equal(t, " literal ", p.Keyword)
	require.Equal(t, " id ", p.SessionID)
}

func TestSessionReadModelPreservesRecordedTagFacetValues(t *testing.T) {
	h := &handler{cfg: config{MaxLimit: 10000, MaxQueryRange: time.Hour}}
	p, err := h.sessionReadModelParams(httptest.NewRequest("GET", "/?space_id=space&agent_uuid=agent&projection_context="+strings.Repeat("a", 64)+"&from=100&to=200&tags=%5B%22%20tag%20%22,%22tag%22%5D", nil))
	require.NoError(t, err)
	require.Equal(t, []string{" tag ", "tag"}, p.Tags)
	_, args := sessionMatchSQL(p, "")
	require.Contains(t, args, `" tag "`)
	require.Contains(t, args, `"tag"`)
}

func TestSessionReadModelAcceptsCanonicalPythonWireOrder(t *testing.T) {
	h := &handler{cfg: config{MaxLimit: 10000, MaxQueryRange: time.Hour}}
	base := "/?space_id=space&agent_uuid=agent&projection_context=" + strings.Repeat("a", 64) + "&from=100&to=200&page=1&limit=20&agent_product=Codex&user=owner&session_semantics=canonical-trace-v1&session_keyword=id&session_status=error&min_trace_count=2&min_total_tokens=0&min_duration_ms=1000&env=prod&username=sdk-user&tags=%5B%22tag%22%5D&keyword=needle&searchType=input"
	for _, suffix := range []string{"&orderBy=end_time_unix_nano.desc", "&order_by=last_activity.desc"} {
		p, err := h.sessionReadModelParams(httptest.NewRequest("GET", base+suffix, nil))
		require.NoError(t, err, suffix)
		require.Equal(t, "last_activity.desc", p.OrderBy)
	}
	for _, suffix := range []string{"&orderBy=start_time_unix_nano.asc", "&orderBy=last_activity.asc", "&orderBy=last_activity.desc&order_by=end_time_unix_nano.desc"} {
		_, err := h.sessionReadModelParams(httptest.NewRequest("GET", base+suffix, nil))
		require.Error(t, err, suffix)
	}
}

func TestSessionReadModelRejectsPayloadBeforeMaterializingRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery("SELECT session_id,COUNT").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("large-session", 1, 123))
	mock.ExpectQuery("SELECT p\\.session_id,COUNT").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("large-session", 1, sessionPagePayloadLimit+1))
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("session_page_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

// Retired strategy flags cannot disable the standard JSON_TABLE compatibility
// path. Every group consumer agrees, including a zero threshold; nil skips usage parsing.
func TestSessionTokenQueryStrategyUsesActualPagePath(t *testing.T) {
	zero, positive := int64(0), int64(100)
	for _, tc := range []struct {
		name, flag string
		threshold  *int64
		optimized  bool
	}{
		{"default", "", &zero, true},
		{"retired_false_ignored", "false", &zero, true},
		{"retired_uppercase_ignored", "TRUE", &zero, true},
		{"enabled_nil", "true", nil, false},
		{"enabled_zero", "true", &zero, true},
		{"enabled_positive", "true", &positive, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SIGNOZ_AIVISION_SESSION_TOKEN_QUERY_OPTIMIZATION_ENABLED", tc.flag)
			t.Setenv("SIGNOZ_AIVISION_TRACE_READ_MODEL_ENABLED", "false")
			t.Setenv("SIGNOZ_AIVISION_TRACE_READ_MODEL_TRACKING_ENABLED", "false")
			t.Setenv("SIGNOZ_AIVISION_TRACE_READ_MODEL_AUTHORITATIVE_INGEST", "true")
			matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
				if expected == "group" || expected == "window" {
					optimized := strings.Contains(actual, "JSON_TABLE(p.row_json,")
					want := tc.optimized && expected == "group"
					if optimized != want {
						return fmt.Errorf("%s query JSON_TABLE active=%t, want %t", expected, optimized, want)
					}
					if optimized && (strings.Contains(actual, "JSON_EXTRACT(p.row_json,'$.usage.total')") || !strings.Contains(actual, "total_tokens JSON PATH '$.usage.total' NULL ON EMPTY ERROR ON ERROR")) {
						return fmt.Errorf("optimized query must preserve JSON extraction types")
					}
					return nil
				}
				if !strings.Contains(actual, expected) {
					return fmt.Errorf("missing expected query %s", expected)
				}
				return nil
			})
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			s := NewModule(factory.ProviderSettings{}, nil, nil, telemetrystore.Config{}).store
			s.db, s.cfg.TablePrefix = db, "test"
			p := sessionReadModelTestParams()
			p.MinTotalTokens = tc.threshold
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "space").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
			mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
			if tc.threshold != nil {
				mock.ExpectQuery("SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}))
			}
			mock.ExpectQuery("group").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectQuery("window").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
			mock.ExpectQuery("group").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}))
			for range 3 {
				mock.ExpectQuery("group").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}))
			}
			mock.ExpectCommit()
			result, err := s.sessionReadModelPage(context.Background(), "org", p)
			require.NoError(t, err)
			strategy := "not_requested"
			if tc.optimized {
				strategy = "json_table"
			}
			require.Equal(t, strategy, result.(map[string]any)["meta"].(map[string]any)["sessionTokenQueryStrategy"])
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
