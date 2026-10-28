package aivision

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/stretchr/testify/require"
)

func TestSessionContentMatchKeepsCompleteIOForPythonVerification(t *testing.T) {
	s, mock := sessionPageBatchStore(t, "true")
	p := sessionReadModelTestParams()
	p.Keyword, p.SearchType = "tail-only-needle", "input"
	sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("s", 1, 150), 1, true)
	mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("s", 1, 4096))
	input := strings.Repeat("prefix ", 200) + "tail-only-needle"
	row := map[string]any{"trace_id": "01", "name": "turn", "userId": "owner", "session_id": "s", "environment": "prod", "environmentSource": "root-span-resource-v1", "start_time_unix_nano": "100000000", "end_time_unix_nano": "101000000", "duration_ms": 1, "status_code": 1, "span_count": 1, "usage": map[string]any{"total": 0}, "coverage": map[string]any{}, "reportedUsage": map[string]any{}, "rootObservationId": "root", "rootSelectionReason": "root_span", "missingParentIds": []string{}, "_traceTags": []string{}, "_sceneValues": []string{}, "resource.attributes.aivision.agent.id": "agent", "resource.attributes.aivision.space.id": "space", "attributes.gen_ai.input.messages": input, "attributes.gen_ai.output.messages": "full answer"}
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	preview := `{"version":"native-trace-preview-v1","complete":true,"input":"bounded prefix","output":"bounded answer","inputTruncated":true,"outputTruncated":true,"rootObservationId":"root"}`
	mock.ExpectQuery(`SELECT p\.session_id AS session_id`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_id", "row_json", "preview_json", "source_revision", "matched"}).AddRow("s", "01", string(encoded), preview, 7, 1))
	sessionPageBatchExpectFacets(mock, 0)
	mock.ExpectCommit()
	result, err := s.sessionReadModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	trace := result.(map[string]any)["data"].([]map[string]any)[0]["traces"].([]map[string]any)[0]
	require.True(t, strings.HasSuffix(trace["attributes.gen_ai.input.messages"].(string), "tail-only-needle"))
	require.Equal(t, input, trace["attributes.gen_ai.input.messages"])
	require.Equal(t, "full answer", trace["attributes.gen_ai.output.messages"])
	require.NoError(t, mock.ExpectationsWereMet())
}

// A middle-turn SDK/content match must retain every scoped turn; exact Session
// IDs differing only by padding/case must remain separate, in page order.
func TestStandardSessionBatchPreservesExactGroupsAndUsage(t *testing.T) {
	batch := sessionPageBatchResult(t, "", true)
	require.Equal(t, "batch", batch["meta"].(map[string]any)["sessionPageSummaryStrategy"])
	data := batch["data"].([]map[string]any)
	require.Equal(t, []string{" s ", "s", "S"}, []string{data[0]["session_id"].(string), data[1]["session_id"].(string), data[2]["session_id"].(string)})
	require.Equal(t, int64(2), data[0]["traceCount"])
	require.Len(t, data[0]["traces"], 2)
	require.Equal(t, []string{"02"}, data[0]["matchedTraceIds"])
	require.Equal(t, json.Number("9007199254740993"), data[0]["traces"].([]map[string]any)[0]["usage"].(map[string]any)["total"])
}

func sessionPageBatchResult(t *testing.T, flag string, batch bool) map[string]any {
	t.Helper()
	s, mock := sessionPageBatchStore(t, flag)
	p := sessionReadModelTestParams()
	p.Username, p.Keyword, p.SearchType = " user ", " needle ", "input"
	groups := sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow(" s ", 2, 150).AddRow("s", 1, 140).AddRow("S", 1, 130)
	sessionPageBatchExpectPage(mock, groups, 3, true)
	mock.ExpectQuery(`SELECT p\.session_id,COUNT\(\*\) AS trace_count,COALESCE\(SUM\(OCTET_LENGTH\(JSON_REMOVE`).WithArgs(sessionPageBatchScopeArgs(" s ", "s", "S")...).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("S", 1, 300).AddRow("s", 1, 300).AddRow(" s ", 2, 600))
	args := append([]driver.Value{" user ", " needle "}, sessionPageBatchScopeArgs(" s ", "s", "S")...)
	args = append(args, sessionPageTraceLimit+1)
	mock.ExpectQuery(`SELECT p\.session_id AS session_id,p\.trace_id AS trace_id,JSON_REMOVE`).WithArgs(args...).WillReturnRows(sessionPageBatchTraceRows(true, []string{" s ", " s ", "S", "s"}, []string{"01", "02", "04", "03"}))
	sessionPageBatchExpectFacets(mock)
	mock.ExpectCommit()
	result, err := s.sessionReadModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
	return result.(map[string]any)
}

func sessionPageBatchStore(t *testing.T, flag string) (*oceanBaseStore, sqlmock.Sqlmock) {
	t.Helper()
	t.Setenv("SIGNOZ_AIVISION_SESSION_PAGE_BATCH_ENABLED", flag)
	t.Setenv("SIGNOZ_AIVISION_TRACE_READ_MODEL_AUTHORITATIVE_INGEST", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s := NewModule(factory.ProviderSettings{}, nil, nil, telemetrystore.Config{}).store
	s.db, s.cfg.TablePrefix = db, "test"
	return s, mock
}

func sessionPageBatchExpectPage(mock sqlmock.Sqlmock, groups *sqlmock.Rows, total int, content ...bool) {
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "space").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	if len(content) > 0 && content[0] {
		expectContentCapability(mock, false)
	}
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(total))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(total))
	mock.ExpectQuery("SELECT session_id,COUNT").WillReturnRows(groups)
}

func sessionPageBatchExpectFacets(mock sqlmock.Sqlmock, cardinality ...int) {
	if len(cardinality) > 0 && cardinality[0] == 0 {
		for _, prefix := range []string{"SELECT p.environment AS value", "SELECT p.username AS value", "SELECT j.value AS value"} {
			mock.ExpectQuery(prefix).WillReturnRows(sqlmock.NewRows([]string{"value", "count"}))
		}
		return
	}
	mock.ExpectQuery("SELECT p.environment AS value").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}).AddRow("prod", 3))
	mock.ExpectQuery("SELECT p.username AS value").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}).AddRow(" user ", 2))
	mock.ExpectQuery("SELECT j.value AS value").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}).AddRow(" tag ", 3))
}

func sessionPageBatchScopeArgs(ids ...string) []driver.Value {
	args := []driver.Value{"org", "space", "agent", int64(100000000), int64(200000000), "agent", "product", "owner"}
	for _, sid := range ids {
		args = append(args, sid, sid)
	}
	return args
}

func sessionPageBatchTraceRows(batch bool, sids, ids []string) *sqlmock.Rows {
	columns := []string{"row_json", "preview_json", "source_revision", "matched"}
	if batch {
		columns = append([]string{"session_id", "trace_id"}, columns...)
	}
	rows := sqlmock.NewRows(columns)
	for i, id := range ids {
		rowValue := map[string]any{
			"trace_id": id, "name": "turn", "userId": " user ", "session_id": sids[i],
			"environment": "prod", "environmentSource": "root-span-resource-v1",
			"start_time_unix_nano": "100000001", "end_time_unix_nano": "100000002", "duration_ms": 0.000001,
			"status_code": 1, "span_count": 1,
			"usage":         map[string]any{"input": json.Number("9007199254740993"), "output": 0, "total": json.Number("9007199254740993"), "cacheReadInput": nil, "cacheWriteInput": nil, "reasoning": nil, "observationCount": 1, "unknownReason": nil, "cost": map[string]any{"total": nil, "input": nil, "output": nil, "currency": "USD", "reason": "cost_missing"}},
			"coverage":      map[string]any{"billableCalls": 1, "knownUsageCalls": 1, "knownTokens": json.Number("9007199254740993"), "knownCostCalls": 0},
			"reportedUsage": map[string]any{}, "total_tokens": json.Number("9007199254740993"),
			"rootObservationId": "root", "rootSelectionReason": "root_span", "missingParentIds": []string{},
			"_traceTags": []string{" tag "}, "_sceneValues": []string{},
			"resource.attributes.aivision.agent.id": "agent", "resource.attributes.aivision.space.id": "space", "resource.attributes.ant.agent.product": "product", "attributes.gen_ai.request.model": "model",
		}
		if id == "01" {
			rowValue["userId"] = "other"
		}
		encoded, _ := json.Marshal(rowValue)
		row := string(encoded)
		preview := `{"version":"native-trace-preview-v1","complete":true,"input":"bounded","output":"answer","inputTruncated":false,"outputTruncated":false,"rootObservationId":"root"}`
		matched := int64(1)
		if id == "01" {
			matched = 0
		}
		values := []driver.Value{row, preview, int64(7), matched}
		if batch {
			values = append([]driver.Value{sids[i], id}, values...)
		}
		rows.AddRow(values...)
	}
	return rows
}

func TestSessionPageBatchRejectsInvalidPreflight(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rows   *sqlmock.Rows
		reason string
	}{
		{"missing", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}), ""},
		{"unexpected_exact_id", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("s", 1, 10), ""},
		{"duplicate", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow(" s ", 1, 10).AddRow(" s ", 1, 10), ""},
		{"wrong_count", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow(" s ", 2, 10), ""},
		{"negative_payload", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow(" s ", 1, -1), "session_page_payload_limit"},
		{"oversized_payload", sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow(" s ", 1, sessionPagePayloadLimit+1), "session_page_payload_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, mock := sessionPageBatchStore(t, "true")
			sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow(" s ", 1, 150), 1)
			mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(tc.rows)
			mock.ExpectRollback()
			result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			if tc.reason == "" {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, readModelUnsupported(tc.reason), result)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSessionPageBatchRejectsUnexpectedOrIncompleteFetch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sids, ids []string
	}{
		{"missing", nil, nil},
		{"wrong_padding", []string{"s"}, []string{"01"}},
		{"extra", []string{" s ", " s "}, []string{"01", "02"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, mock := sessionPageBatchStore(t, "true")
			sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow(" s ", 1, 150), 1)
			mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow(" s ", 1, 100))
			mock.ExpectQuery(`SELECT p\.session_id AS session_id`).WillReturnRows(sessionPageBatchTraceRows(true, tc.sids, tc.ids))
			mock.ExpectRollback()
			result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			require.Error(t, err)
			require.Nil(t, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSessionPageBatchEmptyPageSkipsSummaryReads(t *testing.T) {
	for _, flag := range []string{"", "false", "TRUE", "true"} {
		t.Run("flag_"+flag, func(t *testing.T) {
			s, mock := sessionPageBatchStore(t, flag)
			sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}), 0)
			sessionPageBatchExpectFacets(mock, 0)
			mock.ExpectCommit()
			result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			require.NoError(t, err)
			require.Empty(t, result.(map[string]any)["data"])
			strategy := "batch"
			if flag == "true" {
				strategy = "batch"
			}
			require.Equal(t, strategy, result.(map[string]any)["meta"].(map[string]any)["sessionPageSummaryStrategy"])
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSessionPageBatchRejectsInvalidPageBeforeSummaryReads(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups *sqlmock.Rows
		reason string
	}{
		{"too_many_traces", sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 3000, 150).AddRow("b", 2001, 140), "session_page_trace_limit"},
		{"duplicate_exact_session", sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 1, 150).AddRow("a", 1, 140), ""},
		{"invalid_count", sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 0, 150), ""},
		{"empty_identity", sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("", 1, 150), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, mock := sessionPageBatchStore(t, "true")
			sessionPageBatchExpectPage(mock, tc.groups, 2)
			mock.ExpectRollback()
			result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			if tc.reason == "" {
				require.Error(t, err)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.Equal(t, readModelUnsupported(tc.reason), result)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSessionPageBatchTwentySessionsUseTwoSummaryReads(t *testing.T) {
	s, mock := sessionPageBatchStore(t, "true")
	groups := sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"})
	preflight := sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"})
	ids, traces := []string{}, []string{}
	for i := 0; i < 20; i++ {
		sid := fmt.Sprintf("session-%02d", i)
		groups.AddRow(sid, 1, 200-i)
		preflight.AddRow(sid, 1, 300)
		ids, traces = append(ids, sid), append(traces, fmt.Sprintf("%02x", 100+i))
	}
	sessionPageBatchExpectPage(mock, groups, 20)
	mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(preflight)
	mock.ExpectQuery(`SELECT p\.session_id AS session_id`).WillReturnRows(sessionPageBatchTraceRows(true, ids, traces))
	sessionPageBatchExpectFacets(mock)
	mock.ExpectCommit()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Len(t, result.(map[string]any)["data"], 20)
	require.NoError(t, mock.ExpectationsWereMet(), "20 groups must use the two expected summary queries, not 40")
}

func TestSessionPageBatchRejectsCombinedPayloadBeforeFetch(t *testing.T) {
	s, mock := sessionPageBatchStore(t, "true")
	sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 1, 150).AddRow("b", 1, 140), 2)
	mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("a", 1, sessionPagePayloadLimit/2).AddRow("b", 1, sessionPagePayloadLimit/2+1))
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("session_page_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSessionPageBatchRejectsDuplicateTraceIdentity(t *testing.T) {
	s, mock := sessionPageBatchStore(t, "true")
	sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 2, 150), 1)
	mock.ExpectQuery(`SELECT p\.session_id,COUNT`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("a", 2, 600))
	mock.ExpectQuery(`SELECT p\.session_id AS session_id`).WillReturnRows(sessionPageBatchTraceRows(true, []string{"a", "a"}, []string{"01", "01"}))
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
	require.Error(t, err)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSessionPageBatchRejectsPageAboveSessionLimit(t *testing.T) {
	s, mock := sessionPageBatchStore(t, "true")
	p := sessionReadModelTestParams()
	p.Limit = 50
	groups := sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"})
	for i := 0; i < 51; i++ {
		groups.AddRow(fmt.Sprintf("session-%02d", i), 1, 200-i)
	}
	sessionPageBatchExpectPage(mock, groups, 51)
	mock.ExpectRollback()
	result, err := s.sessionReadModelPage(context.Background(), "org", p)
	require.Error(t, err)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSessionPageBatchStopsOnSummaryDatabaseErrors(t *testing.T) {
	for _, phase := range []string{"preflight", "fetch"} {
		t.Run(phase, func(t *testing.T) {
			s, mock := sessionPageBatchStore(t, "true")
			sessionPageBatchExpectPage(mock, sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}).AddRow("a", 1, 150), 1)
			failure := fmt.Errorf("summary database unavailable")
			q := mock.ExpectQuery(`SELECT p\.session_id,COUNT`)
			if phase == "preflight" {
				q.WillReturnError(failure)
			} else {
				q.WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("a", 1, 300))
				mock.ExpectQuery(`SELECT p\.session_id AS session_id`).WillReturnError(failure)
			}
			mock.ExpectRollback()
			result, err := s.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			require.ErrorIs(t, err, failure)
			require.Nil(t, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
