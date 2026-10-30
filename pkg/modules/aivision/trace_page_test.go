package aivision

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	cmock "github.com/SigNoz/clickhouse-go-mock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

func tracePageTestRows(t *testing.T) []map[string]any {
	t.Helper()
	span := map[string]any{
		"trace_id": "trace-a", "span_id": "span-a", "name": "root", "kind": "internal",
		"start_time_unix_nano": json.Number("1790812800000000001"),
		"end_time_unix_nano":   json.Number("1790812800000000101"), "status_code": 1,
		"attributes":          `{"integer":9223372036854775807,"fraction":0.12345678901234567890123456789,"small":1e-400}`,
		"resource_attributes": `{"aivision.space.id":"space-a"}`, "events": `[]`, "links": `[]`,
		"payload": `{"attributes":{"null":null}}`,
	}
	return []map[string]any{
		{"kind": "meta", "result": map[string]any{"totalItems": 1, "invalidScope": 0, "oversizedTraces": 0, "unsupportedValues": 0}},
		{"kind": "facet", "result": map[string]any{"dimension": "env", "values": []any{map[string]any{"value": "prod", "count": 1}}}},
		{"kind": "trace", "result": map[string]any{"traceId": "trace-a", "spanCount": 1, "spans": []any{span}}},
	}
}

func TestTracePageDecodesExactOTLPJSONAndCompleteness(t *testing.T) {
	result, err := decodeTracePage(tracePageTestRows(t), nativeListParams{Page: 1, Limit: 20, SpaceID: "space-a"})
	require.NoError(t, err)
	page := result.(map[string]any)
	item := page["data"].([]tracePageItem)[0]
	require.Equal(t, 1, item.SpanCount)
	span := item.Spans[0]
	require.Equal(t, "2026-10-01T00:00:00.000000001Z", span["start_time_unix_nano"])
	attrs := span["attributes"].(map[string]any)
	require.Equal(t, json.Number("9223372036854775807"), attrs["integer"])
	require.Equal(t, json.Number("0.12345678901234567890123456789"), attrs["fraction"])
	require.Equal(t, json.Number("1e-400"), attrs["small"])
	require.Equal(t, nativeTracePageContract, page["meta"].(map[string]any)["contract"])
	require.Equal(t, true, page["meta"].(map[string]any)["complete"])
}

func TestTracePageRejectsBrokenCompletenessProof(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]map[string]any) []map[string]any
	}{
		{"scope", func(rows []map[string]any) []map[string]any {
			rows[0]["result"].(map[string]any)["invalidScope"] = 1
			return rows
		}},
		{"oversized", func(rows []map[string]any) []map[string]any {
			rows[0]["result"].(map[string]any)["oversizedTraces"] = 1
			return rows
		}},
		{"count", func(rows []map[string]any) []map[string]any {
			rows[2]["result"].(map[string]any)["spanCount"] = 2
			return rows
		}},
		{"missing_page_trace", func(rows []map[string]any) []map[string]any { return rows[:2] }},
		{"missing_metadata", func(rows []map[string]any) []map[string]any { return rows[1:] }},
		{"duplicate_metadata", func(rows []map[string]any) []map[string]any { return append(rows, rows[0]) }},
		{"missing_scope_proof", func(rows []map[string]any) []map[string]any {
			delete(rows[0]["result"].(map[string]any), "invalidScope")
			return rows
		}},
		{"fractional_scope_proof", func(rows []map[string]any) []map[string]any {
			rows[0]["result"].(map[string]any)["invalidScope"] = json.Number("0.5")
			return rows
		}},
		{"fractional_total", func(rows []map[string]any) []map[string]any {
			rows[0]["result"].(map[string]any)["totalItems"] = json.Number("1.5")
			return rows
		}},
		{"negative_total", func(rows []map[string]any) []map[string]any {
			rows[0]["result"].(map[string]any)["totalItems"] = -1
			return rows
		}},
		{"missing_spans", func(rows []map[string]any) []map[string]any {
			delete(rows[2]["result"].(map[string]any), "spans")
			return rows
		}},
		{"empty_span_id", func(rows []map[string]any) []map[string]any {
			rows[2]["result"].(map[string]any)["spans"].([]any)[0].(map[string]any)["span_id"] = ""
			return rows
		}},
		{"fractional_span_count", func(rows []map[string]any) []map[string]any {
			rows[2]["result"].(map[string]any)["spanCount"] = json.Number("1.5")
			return rows
		}},
		{"unknown_facet", func(rows []map[string]any) []map[string]any {
			rows[1]["result"].(map[string]any)["dimension"] = "unrecognized"
			return rows
		}},
		{"fractional_facet_count", func(rows []map[string]any) []map[string]any {
			rows[1]["result"].(map[string]any)["values"].([]any)[0].(map[string]any)["count"] = json.Number("1.5")
			return rows
		}},
		{"unknown_row_kind", func(rows []map[string]any) []map[string]any {
			rows[1]["kind"] = "unexpected"
			return rows
		}},
		{"duplicate_trace", func(rows []map[string]any) []map[string]any { return append(rows, rows[2]) }},
		{"duplicate_span", func(rows []map[string]any) []map[string]any {
			item := rows[2]["result"].(map[string]any)
			item["spanCount"] = 2
			spans := item["spans"].([]any)
			item["spans"] = append(spans, spans[0])
			return rows
		}},
		{"foreign_trace", func(rows []map[string]any) []map[string]any {
			rows[2]["result"].(map[string]any)["spans"].([]any)[0].(map[string]any)["trace_id"] = "other"
			return rows
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := decodeTracePage(test.mutate(tracePageTestRows(t)), nativeListParams{Page: 1, Limit: 20})
			require.Error(t, err)
			require.Nil(t, result, "invalid data must not be mistaken for an unsupported query")
		})
	}
}

func TestTracePageUnsupportedIsExplicit(t *testing.T) {
	base := nativeListParams{From: 1700000000000, To: 1700003600000, Page: 1, Limit: 20, SpaceID: "space-a"}
	for _, mutate := range []func(*nativeListParams){
		func(p *nativeListParams) { p.OrderBy = "input_tokens.desc" },
		func(p *nativeListParams) { p.SearchType = "input" },
		func(p *nativeListParams) { p.SessionID = "session-a" },
		func(p *nativeListParams) { p.Keyword = strings.Repeat("a", 4097) },
		func(p *nativeListParams) { p.Filters = []nativeFilter{{Field: "name", Value: "root"}} },
		func(p *nativeListParams) { p.Filters = make([]nativeFilter, 101) },
		func(p *nativeListParams) { p.Tags = make([]string, 101) },
		func(p *nativeListParams) {
			p.Filters = []nativeFilter{{Field: "trace_id", Op: "in", Value: make([]string, 201)}}
		},
	} {
		p := base
		mutate(&p)
		_, _, err := buildTracePageSQL("traces", "org-a", p, 10000)
		require.IsType(t, tracePageUnsupported{}, err)
	}
	rows := tracePageTestRows(t)
	rows[0]["result"].(map[string]any)["unsupportedValues"] = 1
	result, err := decodeTracePage(rows, base)
	require.NoError(t, err)
	require.Equal(t, false, result.(map[string]any)["supported"])
	require.NotContains(t, result.(map[string]any), "meta")
}

func TestTracePageSQLBindsIdentityAndFilterValues(t *testing.T) {
	p := nativeListParams{From: 1700000000000, To: 1700003600000, Page: 1, Limit: 20, SpaceID: "space-a", AgentUUID: "agent-a", Keyword: "' OR 1=1 --"}
	sql, args, err := buildTracePageSQL("traces", "org-a", p, 10000)
	require.NoError(t, err)
	require.NotContains(t, sql, p.Keyword)
	require.Contains(t, args, p.Keyword)
	require.Equal(t, strings.Count(sql, "?"), len(args))
}

type tracePageUnitStore struct {
	telemetrystore.TelemetryStore
	queries int
	args    []any
	rows    telemetrystore.Rows
}

func (s *tracePageUnitStore) QueryContext(_ context.Context, _ string, args ...any) (telemetrystore.Rows, error) {
	s.queries++
	s.args = append([]any(nil), args...)
	if s.rows == nil {
		return nil, context.DeadlineExceeded
	}
	return s.rows, nil
}

func tracePageUnitHandler(store *tracePageUnitStore) *handler {
	cfg := config{Enabled: true, MaxLimit: 10000, MaxQueryRange: 31 * 24 * time.Hour, QueryTimeout: time.Second}
	return &handler{backend: &oceanBaseStore{telemetryStore: store, cfg: cfg}, cfg: cfg, logger: slog.Default()}
}

const tracePageUnitOrg = "00000000-0000-4000-8000-000000000001"

func tracePageUnitRequest(params url.Values, authenticated bool) *http.Request {
	query := url.Values{"from": {"1790812800000"}, "to": {"1790816400000"}, "space_id": {"space-a"}}
	for key, values := range params {
		query[key] = values
	}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/page?"+query.Encode(), nil)
	if authenticated {
		r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: tracePageUnitOrg}))
	}
	return r
}

func TestTracePageHandlerAuthenticatesBeforeCapabilityResponse(t *testing.T) {
	for _, org := range []string{"missing", "invalid-uuid", "00000000-0000-0000-0000-000000000000"} {
		t.Run(org, func(t *testing.T) {
			store := &tracePageUnitStore{}
			r := tracePageUnitRequest(url.Values{"unknown_capability": {"true"}}, false)
			r.Header.Set("X-SigNoz-OrgId", tracePageUnitOrg)
			if org != "missing" {
				r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: org}))
			}
			w := httptest.NewRecorder()
			tracePageUnitHandler(store).TracePage(w, r)
			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), `"supported":false`)
			require.Zero(t, store.queries)
		})
	}
}

func TestTracePageHandlerUnsupportedQueriesNeverTouchStorage(t *testing.T) {
	for _, params := range []url.Values{
		{"unknown_capability": {"true"}},
		{"orderBy": {"input_tokens.desc"}},
		{"searchType": {"input"}},
		{"filters": {`[{"field":"name","op":"=","value":"root"}]`}},
	} {
		t.Run(params.Encode(), func(t *testing.T) {
			store := &tracePageUnitStore{}
			w := httptest.NewRecorder()
			tracePageUnitHandler(store).TracePage(w, tracePageUnitRequest(params, true))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var response struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
			require.Equal(t, false, response.Data["supported"])
			require.NotEmpty(t, response.Data["reason"])
			require.NotContains(t, response.Data, "meta")
			require.Zero(t, store.queries)
		})
	}
}

func TestTracePageHandlerRejectsInvalidRequestsBeforeStorage(t *testing.T) {
	for name, params := range map[string]url.Values{
		"malformed_filters":       {"filters": {`[{broken`}},
		"non_array_filters":       {"filters": {`{"field":"trace_id"}`}},
		"second_filter_document":  {"filters": {`[] [{"field":"trace_id","value":"other"}]`}},
		"trailing_filter_garbage": {"filters": {`[] trailing`}},
		"malformed_tags":          {"tags": {`["sdk"] []`}},
		"page_zero":               {"page": {"0"}},
		"page_not_integer":        {"page": {"1.5"}},
		"limit_zero":              {"limit": {"0"}},
		"limit_too_large":         {"limit": {"201"}},
		"missing_space":           {"space_id": {""}},
	} {
		t.Run(name, func(t *testing.T) {
			store := &tracePageUnitStore{}
			w := httptest.NewRecorder()
			tracePageUnitHandler(store).TracePage(w, tracePageUnitRequest(params, true))
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), `"complete":true`)
			require.Zero(t, store.queries)
		})
	}
}

func TestTracePageHandlerRejectsWritesBeforeStorage(t *testing.T) {
	store := &tracePageUnitStore{}
	r := tracePageUnitRequest(nil, true)
	r.Method = http.MethodPost
	w := httptest.NewRecorder()
	tracePageUnitHandler(store).TracePage(w, r)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)
	require.Equal(t, http.MethodGet, w.Header().Get("Allow"))
	require.Zero(t, store.queries)
}

func TestTracePageHandlerPreservesExactFiltersAndClaimsAtStorageBoundary(t *testing.T) {
	const envelope = `[{"section":0,"pageRank":0,"kind":"meta","result":{"totalItems":0,"invalidScope":0,"oversizedTraces":0,"unsupportedValues":0}}]`
	store := &tracePageUnitStore{rows: telemetrystore.AdaptClickHouseRows(telemetrystore.WrapRows(cmock.NewRows(
		[]cmock.ColumnType{{Name: "trace_page_rows", Type: "String"}}, [][]any{{envelope}},
	)), nil)}
	params := url.Values{
		"filters": {`[{"field":"start_time_unix_nano","op":">=","value":1790812800000000001}]`},
		"keyword": {" trace "}, "env": {" prod "}, "tags": {`[" sdk "]`},
		"user": {"subject-a"}, "agent_product": {"Codex"}, "agent_uuid": {"agent-a"},
	}
	r := tracePageUnitRequest(params, true)
	r.Header.Set("X-SigNoz-OrgId", "00000000-0000-4000-8000-000000000002")
	w := httptest.NewRecorder()
	h := tracePageUnitHandler(store)
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	defer db.Close()
	h.backend.db = db
	h.backend.cfg.TablePrefix = "test"
	mock.ExpectBegin()
	for _, col := range [][3]string{{ingest.TracePageDimensionsColumn, "longtext", ingest.TracePageDimensionsExpression}, {ingest.TracePageSourceBytesColumn, "bigint", ingest.TracePageSourceBytesExpression}} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_traces", col[0]).WillReturnRows(sqlmock.NewRows([]string{"type", "extra", "expression"}).AddRow(col[1], "STORED GENERATED", col[2]))
	}
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max"}).AddRow(0, 0, 0))
	mock.ExpectQuery("WITH base").WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(4096))
	// Capture the bound values at the transactional database boundary.
	mock.ExpectQuery("WITH base").WillReturnRows(sqlmock.NewRows([]string{"trace_page_rows"}).AddRow(envelope))
	mock.ExpectCommit()
	h.TracePage(w, r)
	require.NoError(t, mock.ExpectationsWereMet())
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	parsed, err := h.nativeParams(r)
	require.NoError(t, err)
	parsed.AgentUUID = "agent-a"
	parsed.Env = " prod "
	parsed.Keyword = " trace "
	parsed.Tags = []string{" sdk "}
	parsed.Filters = []nativeFilter{{Field: "start_time_unix_nano", Op: ">=", Value: json.Number("1790812800000000001")}}
	_, bound, err := buildTracePageSQL(h.backend.tracesTable(), tracePageUnitOrg, parsed, 10000)
	require.NoError(t, err)
	for _, value := range []any{tracePageUnitOrg, "space-a", "subject-a", "Codex", "agent-a", int64(1790812800000000001), " trace ", " prod ", " sdk "} {
		require.Contains(t, bound, value, "the bound query must preserve authorized scope and exact values")
	}
	require.NotContains(t, bound, r.Header.Get("X-SigNoz-OrgId"))
	var response struct {
		Data struct {
			Meta map[string]any `json:"meta"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Equal(t, true, response.Data.Meta["complete"])
	require.Equal(t, "space-a", response.Data.Meta["space_id"])
	require.Equal(t, float64(50), response.Data.Meta["limit"])
}

func TestTracePageJSONRejectsMalformedOrMultipleDocuments(t *testing.T) {
	for _, document := range []string{`{broken`, `{}`, `{"totalItems":1}`} {
		if document == `{broken` {
			_, err := tracePageJSON(document)
			require.Error(t, err)
			continue
		}
		for _, suffix := range []string{` {}`, ` []`, ` trailing`} {
			_, err := tracePageJSON(document + suffix)
			require.Error(t, err, "only one complete metadata document is valid")
		}
	}
	value, err := tracePageJSON([]byte("{\"value\":1790812800000000001} \n\t"))
	require.NoError(t, err)
	require.Equal(t, json.Number("1790812800000000001"), value["value"])
}

func TestTracePageDoesNotParseNonPagePayloads(t *testing.T) {
	query, _, err := buildTracePageSQL("traces", "org", nativeListParams{From: 1790985600000, To: 1791244800000, Page: 1, Limit: 50, SpaceID: "space"}, 10000)
	require.NoError(t, err)
	beforePage := strings.Split(query, "), page AS (")[0]
	require.NotContains(t, beforePage, "JSON_EXTRACT(payload")
	require.NotContains(t, beforePage, "SELECT t.*")
	require.Contains(t, beforePage, "page_dimensions_v1")
}
