package aivision

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/telemetrystore/oceanbasetelemetrystore"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/stretchr/testify/require"
)

// This test executes the production handler and SQL against OceanBase. It only
// creates, writes and drops its own uniquely named copy of the traces table.
// Run with SIGNOZ_TEST_OCEANBASE_DSN pointing to a schema with signoz_traces.
// Optionally set SIGNOZ_TEST_AIVISION_BACKEND and SIGNOZ_TEST_NATIVE_PYTHON to
// compare the same fixtures with AI Vision's NativeTraceWindow implementation.
func TestOceanBaseNativeTracePage(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for a real OceanBase integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "sn_page_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	table := "`" + prefix + "_traces`"
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" LIKE `signoz_traces`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP TABLE "+table)
		require.NoError(t, err)
	})
	cfg := telemetrystore.NewConfigFactory().New().(telemetrystore.Config)
	cfg.Provider = "oceanbase"
	cfg.OceanBase.DSN = dsn
	cfg.OceanBase.TablePrefix = prefix
	settings := instrumentationtest.New().ToProviderSettings()
	backend, err := oceanbasetelemetrystore.New(ctx, settings, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.(interface{ Close() error }).Close()) })
	require.NoError(t, ingest.InitializeTracePageDimensions(ctx, db, prefix))
	h := NewHandler(NewModule(settings, backend, nil, cfg))
	var lastWire string
	fixture := tracePageIntegrationFixture{
		db: db, table: table, org: valuer.GenerateUUID().StringValue(),
		start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), lastWire: &lastWire,
	}
	query := func(t *testing.T, scope string, extra url.Values) map[string]any {
		t.Helper()
		status, payload := fixture.call(t, h.TracePage, scope, extra)
		require.Equal(t, http.StatusOK, status, payload)
		require.Equal(t, "success", payload["status"], payload)
		result, ok := payload["data"].(map[string]any)
		require.True(t, ok, payload)
		meta, ok := result["meta"].(map[string]any)
		require.True(t, ok, result)
		require.Equal(t, "native-trace-page-v1", meta["contract"])
		require.Equal(t, true, meta["complete"])
		return result
	}

	for _, count := range []int{211, 1000} {
		t.Run(fmt.Sprintf("stable_pages_%d", count), func(t *testing.T) {
			scope := fmt.Sprintf("scale-%d", count)
			spans := make([]tracePageIntegrationSpan, 0, 2*count)
			for i := 0; i < count; i++ {
				root, child := fixture.pair(scope, i)
				// All traces tie on their timestamp. Paging must use trace ID as
				// a secondary sort, in the same direction as the primary key.
				root.resource["ant.agent.env"] = "prod"
				child.resource["ant.agent.env"] = "test"
				child.resource["ant.agent.scene"] = "coding"
				root.resource["ant.username"] = "subject"
				spans = append(spans, root, child)
			}
			fixture.insert(t, spans)
			seen := map[string]bool{}
			const limit = 20
			for page := 1; page <= (count+limit-1)/limit; page++ {
				result := query(t, scope, url.Values{"page": {strconv.Itoa(page)}, "limit": {"20"}})
				meta := result["meta"].(map[string]any)
				require.EqualValues(t, count, meta["totalItems"])
				require.EqualValues(t, (count+limit-1)/limit, meta["totalPages"])
				items := tracePageIntegrationItems(t, result)
				for index, item := range items {
					id := item["traceId"].(string)
					require.Equal(t, fmt.Sprintf("%032x", count-1-(page-1)*limit-index), id)
					require.False(t, seen[id], "trace repeated across stable pages")
					seen[id] = true
					require.EqualValues(t, 2, item["spanCount"])
					require.Len(t, item["spans"], 2, "page must carry complete traces")
				}
				require.Less(t, len(lastWire), 24*1024*1024, "wire response must fit the checked page budget")
				require.Equal(t, map[string]int{"prod": count, "test": count}, tracePageIntegrationFacet(t, result, "env"))
				require.Equal(t, map[string]int{"coding": count}, tracePageIntegrationFacet(t, result, "scene"))
				if page <= 2 {
					tracePageIntegrationOracle(t, scope, spans, map[string]any{"page": page}, result)
				}
			}
			require.Len(t, seen, count)
			ascending := query(t, scope, url.Values{"orderBy": {"start_time_unix_nano.asc"}, "limit": {"20"}})
			for i, item := range tracePageIntegrationItems(t, ascending) {
				require.Equal(t, fmt.Sprintf("%032x", i), item["traceId"])
			}
			outOfRange := query(t, scope, url.Values{"page": {"999"}})
			require.Empty(t, tracePageIntegrationItems(t, outOfRange))
			require.EqualValues(t, count, outOfRange["meta"].(map[string]any)["totalItems"])
			tracePageIntegrationOracle(t, scope, spans, map[string]any{"page": 999}, outOfRange)
		})
	}

	t.Run("cross_span_predicates_and_resource_username", func(t *testing.T) {
		scope := "cross-span"
		root, child := fixture.pair(scope, 1)
		root.resource["ant.agent.env"] = "prod"
		child.resource["ant.agent.scene"] = "coding"
		root.attributes["ant.username"] = "spoofed"
		child.resource["ant.username"] = "subject"
		nullRoot, nullChild := fixture.pair(scope, 2)
		nullRoot.resource["ant.agent.env"] = "test"
		nullRoot.attributes["ant.username"] = "spoofed"
		nullChild.resource["ant.username"] = nil
		spans := []tracePageIntegrationSpan{root, child, nullRoot, nullChild}
		fixture.insert(t, spans)
		cases := []struct {
			name    string
			filters []nativeFilter
			want    []string
		}{
			{"different_spans", []nativeFilter{{Field: "resource.attributes.ant.agent.env", Op: "=", Value: "prod"}, {Field: "resource.attributes.ant.agent.scene", Op: "=", Value: "coding"}}, []string{root.traceID}},
			{"negative_all_spans", []nativeFilter{{Field: "resource.attributes.ant.agent.env", Op: "!=", Value: "prod"}}, []string{nullRoot.traceID}},
			{"not_in", []nativeFilter{{Field: "resource.attributes.ant.agent.env", Op: "not_in", Value: []string{"prod", "test"}}}, []string{}},
			{"resource_username", []nativeFilter{{Field: "attributes.ant.username", Op: "=", Value: "subject"}}, []string{root.traceID}},
			{"attribute_spoof_ignored", []nativeFilter{{Field: "attributes.ant.username", Op: "=", Value: "spoofed"}}, []string{}},
			{"explicit_null_username", []nativeFilter{{Field: "attributes.ant.username", Op: "IS_NULL"}}, []string{nullRoot.traceID}},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				filters, err := json.Marshal(test.filters)
				require.NoError(t, err)
				result := query(t, scope, url.Values{"filters": {string(filters)}})
				require.Equal(t, test.want, tracePageIntegrationIDs(t, result))
				tracePageIntegrationOracle(t, scope, spans, map[string]any{"filters": test.filters}, result)
			})
		}
		all := query(t, scope, nil)
		require.Equal(t, map[string]int{"subject": 1}, tracePageIntegrationFacet(t, all, "username"))
		filtered := query(t, scope, url.Values{"env": {"prod"}, "scene": {"coding"}, "username": {"subject"}})
		require.Equal(t, []string{root.traceID}, tracePageIntegrationIDs(t, filtered))
		require.Equal(t, map[string]int{"prod": 1}, tracePageIntegrationFacet(t, filtered, "env"), "trace facets retain their own dimension filter")
		require.Len(t, tracePageIntegrationItems(t, filtered)[0]["spans"], 2, "matching child predicates must not discard root spans")
	})

	t.Run("canonical_tag_updates_and_facets", func(t *testing.T) {
		scope := "tag-updates"
		root, child := fixture.pair(scope, 1)
		root.resource["ant.agent.env"] = "prod"
		root.attributes["langfuse.trace.tags"] = []string{"old"}
		child.attributes["langfuse.trace.tags"] = []string{"new", "shared", "new"}
		child.end = root.end + 1
		nullRoot, nullChild := fixture.pair(scope, 2)
		nullRoot.attributes["langfuse.trace.tags"] = []string{"old"}
		nullChild.attributes["langfuse.trace.tags"] = nil
		nullChild.end = nullRoot.end + 1
		encodedRoot, encodedChild := fixture.pair(scope, 3)
		encodedRoot.attributes["langfuse.trace.tags"] = `["encoded"]`
		// Completion order wins over start order: a root may complete last.
		lastRoot, lastChild := fixture.pair(scope, 4)
		lastRoot.attributes["langfuse.trace.tags"] = []string{"root-last"}
		lastChild.attributes["langfuse.trace.tags"] = []string{"child-early"}
		lastRoot.end = lastChild.end + 1
		spacedRoot, spacedChild := fixture.pair(scope, 5)
		spacedRoot.attributes["langfuse.trace.tags"] = []string{" padded "}
		spans := []tracePageIntegrationSpan{root, child, nullRoot, nullChild, encodedRoot, encodedChild, lastRoot, lastChild, spacedRoot, spacedChild}
		fixture.insert(t, spans)
		all := query(t, scope, nil)
		require.Equal(t, map[string]int{"new": 1, "shared": 1, "encoded": 1, "root-last": 1, " padded ": 1}, tracePageIntegrationFacet(t, all, "tags"))
		tracePageIntegrationOracle(t, scope, spans, map[string]any{}, all)
		selected := query(t, scope, url.Values{"tags": {`["missing","new"]`}})
		require.Equal(t, []string{root.traceID}, tracePageIntegrationIDs(t, selected), "tag multiselect uses OR")
		require.Equal(t, map[string]int{"new": 1, "shared": 1}, tracePageIntegrationFacet(t, selected, "tags"))
		tracePageIntegrationOracle(t, scope, spans, map[string]any{"tags": []string{"missing", "new"}}, selected)
		cleared := query(t, scope, url.Values{"tags": {`["old"]`}})
		require.Empty(t, tracePageIntegrationIDs(t, cleared), "explicit null and later updates clear old tags")
		emptyTags := query(t, scope, url.Values{"tags": {`[]`}})
		require.Equal(t, tracePageIntegrationIDs(t, all), tracePageIntegrationIDs(t, emptyTags))
		spaced := query(t, scope, url.Values{"tags": {`[" padded "]`}})
		require.Equal(t, []string{spacedRoot.traceID}, tracePageIntegrationIDs(t, spaced))
		tracePageIntegrationOracle(t, scope, spans, map[string]any{"tags": []string{" padded "}}, spaced)
	})

	t.Run("page_span_total_does_not_consume_per_trace_cap", func(t *testing.T) {
		scope := "span-cap"
		firstRoot, firstChild := fixture.pair(scope, 1)
		secondRoot, secondChild := fixture.pair(scope, 2)
		fixture.insert(t, []tracePageIntegrationSpan{firstRoot, firstChild, secondRoot, secondChild})
		limited := cfg
		limited.OceanBase.MaxResultRows = 2
		handler := NewHandler(NewModule(settings, backend, nil, limited))
		status, payload := fixture.call(t, handler.TracePage, scope, nil)
		require.Equal(t, http.StatusOK, status, payload)
		result := payload["data"].(map[string]any)
		require.Equal(t, true, result["meta"].(map[string]any)["complete"])
		require.Len(t, tracePageIntegrationItems(t, result), 2)
		for _, item := range tracePageIntegrationItems(t, result) {
			require.Len(t, item["spans"], 2)
		}
		third := secondChild
		third.spanID = "0000000000ff0001"
		fixture.insert(t, []tracePageIntegrationSpan{third})
		status, payload = fixture.call(t, handler.TracePage, scope, nil)
		require.GreaterOrEqual(t, status, 400, payload)
		require.NotEqual(t, "success", payload["status"], "a trace above the configured cap must fail rather than truncate")
	})

	t.Run("exact_nanoseconds_and_like_literals", func(t *testing.T) {
		scope := "exact-values"
		root, child := fixture.pair(scope, 1)
		root.resource["ant.agent.env"] = `prod_A\B`
		child.attributes["exact_int64"] = int64(9223372036854775807)
		otherRoot, otherChild := fixture.pair(scope, 2)
		otherRoot.resource["ant.agent.env"] = `prodXA\B`
		spans := []tracePageIntegrationSpan{root, child, otherRoot, otherChild}
		fixture.insert(t, spans)
		for _, filter := range []nativeFilter{
			{Field: "start_time_unix_nano", Op: "=", Value: fixture.start.UnixNano()},
			{Field: "start_time_unix_nano", Op: ">=", Value: fixture.start.UnixNano() + 1},
			{Field: "end_time_unix_nano", Op: "=", Value: child.end},
			{Field: "duration_ms", Op: ">", Value: 1000},
			{Field: "resource.attributes.ant.agent.env", Op: "LIKE", Value: `%_A\B`},
			{Field: "resource.attributes.ant.agent.env", Op: "LIKE", Value: `%_a\b`},
		} {
			filters := []nativeFilter{filter}
			encoded, err := json.Marshal(filters)
			require.NoError(t, err)
			result := query(t, scope, url.Values{"filters": {string(encoded)}})
			tracePageIntegrationOracle(t, scope, spans, map[string]any{"filters": filters}, result)
			switch {
			case filter.Op == ">=" || filter.Value == `%_a\b`:
				require.Empty(t, tracePageIntegrationIDs(t, result))
			case filter.Op == "LIKE":
				require.Equal(t, []string{root.traceID}, tracePageIntegrationIDs(t, result))
			default:
				require.Len(t, tracePageIntegrationItems(t, result), 2)
			}
			if len(tracePageIntegrationItems(t, result)) > 0 {
				require.Contains(t, lastWire, `"exact_int64":9223372036854775807`, "trace page wire must preserve int64 attribute values")
				require.Contains(t, lastWire, `2026-10-01T00:00:00.000000001Z`, "trace page wire must preserve nanosecond timestamps")
			}
		}
	})

	t.Run("invalid_identity_outside_page_fails", func(t *testing.T) {
		for _, kind := range []string{"mixed-agent", "missing-agent", "foreign-space"} {
			t.Run(kind, func(t *testing.T) {
				scope := "invalid-" + kind
				badRoot, badChild := fixture.pair(scope, 0)
				switch kind {
				case "mixed-agent":
					badChild.resource["aivision.agent.id"] = "other-agent"
				case "missing-agent":
					delete(badChild.resource, "aivision.agent.id")
				case "foreign-space":
					badChild.resource["aivision.space.id"] = "other-space"
				}
				goodRoot, goodChild := fixture.pair(scope, 1)
				fixture.insert(t, []tracePageIntegrationSpan{badRoot, badChild, goodRoot, goodChild})
				status, payload := fixture.call(t, h.TracePage, scope, url.Values{"limit": {"1"}})
				require.GreaterOrEqual(t, status, 400, payload)
				require.NotEqual(t, "success", payload["status"], "scope failures cannot trigger compatibility fallback")
			})
		}
	})

	t.Run("empty_window_and_explicit_unsupported", func(t *testing.T) {
		empty := query(t, "empty", nil)
		require.Empty(t, tracePageIntegrationItems(t, empty))
		require.EqualValues(t, 0, empty["meta"].(map[string]any)["totalItems"])
		for _, extra := range []url.Values{
			{"orderBy": {"total_tokens.desc"}},
			{"filters": {`[{"field":"total_tokens","op":">","value":0}]`}},
			{"searchType": {"input"}, "keyword": {"hello"}},
		} {
			status, payload := fixture.call(t, h.TracePage, "scale-211", extra)
			require.Equal(t, http.StatusOK, status, payload)
			data := payload["data"].(map[string]any)
			require.Equal(t, false, data["supported"])
			require.NotEmpty(t, data["reason"])
			require.Nil(t, data["meta"], "unsupported queries must not claim complete results")
		}
	})
}

type tracePageIntegrationStore struct {
	telemetrystore.TelemetryStore
	queries int
	rows    int
}

func (s *tracePageIntegrationStore) QueryContext(ctx context.Context, query string, args ...any) (telemetrystore.Rows, error) {
	s.queries++
	rows, err := s.TelemetryStore.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &tracePageIntegrationRows{Rows: rows, count: &s.rows}, nil
}

type tracePageIntegrationRows struct {
	telemetrystore.Rows
	count *int
}

func (r *tracePageIntegrationRows) Next() bool {
	next := r.Rows.Next()
	if next {
		*r.count++
	}
	return next
}

type tracePageIntegrationFixture struct {
	db       *sql.DB
	table    string
	org      string
	start    time.Time
	lastWire *string
}

type tracePageIntegrationSpan struct {
	scope, traceID, spanID, parent string
	start, end                     int64
	resource, attributes           map[string]any
}

func (f tracePageIntegrationFixture) pair(scope string, index int) (tracePageIntegrationSpan, tracePageIntegrationSpan) {
	span := func(offset int) tracePageIntegrationSpan {
		return tracePageIntegrationSpan{
			scope: scope, traceID: fmt.Sprintf("%032x", index), spanID: fmt.Sprintf("%016x", index*2+offset+1),
			start: f.start.UnixNano() + int64(offset), end: f.start.Add(time.Second).UnixNano() + int64(offset),
			resource:   map[string]any{"aivision.space.id": scope, "aivision.agent.id": "agent"},
			attributes: map[string]any{"gen_ai.operation.name": "invoke_agent"},
		}
	}
	root, child := span(0), span(1)
	child.parent = root.spanID
	child.attributes["gen_ai.operation.name"] = "chat"
	child.attributes["gen_ai.usage.input_tokens"] = 10
	child.attributes["gen_ai.usage.output_tokens"] = 2
	return root, child
}

func (f tracePageIntegrationFixture) insert(t *testing.T, spans []tracePageIntegrationSpan) {
	t.Helper()
	tx, err := f.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	statement, err := tx.Prepare("INSERT INTO " + f.table + " (org_id, space_id, timestamp, trace_id, span_id, parent_span_id, span_name, span_kind, start_time_unix_nano, end_time_unix_nano, duration_nano, scope_attributes, resource_attributes, attributes, events, links, payload, user_id, agent_product, status_code) VALUES (?, ?, ?, ?, ?, ?, 'fixture', 'Internal', ?, ?, ?, '{}', ?, ?, '[]', '[]', '{}', 'subject', 'Codex', 1)")
	require.NoError(t, err)
	defer statement.Close()
	for _, span := range spans {
		resource, err := json.Marshal(span.resource)
		require.NoError(t, err)
		attributes, err := json.Marshal(span.attributes)
		require.NoError(t, err)
		_, err = statement.Exec(f.org, span.scope, span.start, span.traceID, span.spanID, span.parent, span.start, span.end, span.end-span.start, string(resource), string(attributes))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func (f tracePageIntegrationFixture) call(t *testing.T, handle http.HandlerFunc, scope string, extra url.Values) (int, map[string]any) {
	t.Helper()
	params := url.Values{
		"from": {strconv.FormatInt(f.start.UnixMilli(), 10)}, "to": {strconv.FormatInt(f.start.Add(time.Minute).UnixMilli(), 10)},
		"space_id": {scope}, "agent_uuid": {"agent"}, "agent_product": {"Codex"}, "page": {"1"}, "limit": {"20"},
	}
	for key, values := range extra {
		params[key] = values
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/page?"+params.Encode(), nil)
	request = request.WithContext(authtypes.NewContextWithClaims(request.Context(), authtypes.Claims{OrgID: f.org}))
	response := httptest.NewRecorder()
	handle(response, request)
	*f.lastWire = response.Body.String()
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope), response.Body.String())
	return response.Code, envelope
}

func tracePageIntegrationItems(t *testing.T, result map[string]any) []map[string]any {
	t.Helper()
	values, ok := result["data"].([]any)
	require.True(t, ok, result)
	items := make([]map[string]any, 0, len(values))
	for _, value := range values {
		item, ok := value.(map[string]any)
		require.True(t, ok, value)
		items = append(items, item)
	}
	return items
}

func tracePageIntegrationIDs(t *testing.T, result map[string]any) []string {
	t.Helper()
	ids := make([]string, 0)
	for _, item := range tracePageIntegrationItems(t, result) {
		ids = append(ids, item["traceId"].(string))
	}
	sort.Strings(ids)
	return ids
}

func tracePageIntegrationFacet(t *testing.T, result map[string]any, name string) map[string]int {
	t.Helper()
	facets, ok := result["facets"].(map[string]any)
	require.True(t, ok, result)
	values, ok := facets[name].([]any)
	require.True(t, ok, facets)
	counts := map[string]int{}
	for _, value := range values {
		item := value.(map[string]any)
		counts[item["value"].(string)] = int(item["count"].(float64))
	}
	return counts
}

func tracePageIntegrationOracle(t *testing.T, scope string, spans []tracePageIntegrationSpan, params map[string]any, result map[string]any) {
	t.Helper()
	backend := os.Getenv("SIGNOZ_TEST_AIVISION_BACKEND")
	if backend == "" {
		return
	}
	python := os.Getenv("SIGNOZ_TEST_NATIVE_PYTHON")
	if python == "" {
		python = "python3"
	}
	byID := map[string][]map[string]any{}
	for _, span := range spans {
		byID[span.traceID] = append(byID[span.traceID], map[string]any{
			"trace_id": span.traceID, "span_id": span.spanID, "parent_span_id": span.parent,
			"name": "fixture", "start_time_unix_nano": strconv.FormatInt(span.start, 10), "end_time_unix_nano": strconv.FormatInt(span.end, 10),
			"resource": span.resource, "attributes": span.attributes, "status": map[string]any{"code": "OK"},
		})
	}
	details := make([]map[string]any, 0, len(byID))
	for id, values := range byID {
		details = append(details, map[string]any{"traceId": id, "spans": values})
	}
	input, err := json.Marshal(map[string]any{"scope": scope, "details": details, "params": params, "payload": result})
	require.NoError(t, err)
	script := `import json, sys
from apps.core.trace_query.native_query import NativeTraceWindow
value = json.load(sys.stdin)
window = NativeTraceWindow(value["details"], project_id=value["scope"])
params = {"page": 1, "limit": 20, **value["params"]}
result = window.list(params)
from django.conf import settings
settings.configure(INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "apps.core"], DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}, AUTH_USER_MODEL="core.User", SECRET_KEY="integration-test", LOGGING_CONFIG=None, AIVISION_MODEL_PRICES=[])
import django
django.setup()
from types import SimpleNamespace
from unittest.mock import Mock, patch
from apps.core.trace_query.signoz.provider import SignozTraceProvider
from apps.core.trace_query.types import TraceListQuery
client = Mock()
client.get.return_value = value["payload"]
provider = SignozTraceProvider()
request_params = {**params, "from": value["payload"]["meta"]["from"], "to": value["payload"]["meta"]["to"], "agent_uuid": "agent", "agent_product": "Codex"}
query = TraceListQuery(SimpleNamespace(uuid=value["scope"], type=2), None, request_params)
with patch.object(provider, "_context", return_value=(client, {"agent_product_filter": "Codex"})), patch.object(provider, "_window", side_effect=AssertionError("storage page must not invoke compatibility scan")):
    page = provider.query_traces(query)
assert page["meta"]["queryMode"] == "storage", page["meta"]
assert page["meta"]["totalItems"] == result["meta"]["totalItems"], page["meta"]
assert [row["trace_id"] for row in page["data"]] == [row["trace_id"] for row in result["data"]]
assert page["facets"] == window.facets(params), page["facets"]
assert client.get.call_count == 1, client.get.call_count
print(json.dumps({"ids": [row["trace_id"] for row in result["data"]], "total": result["meta"]["totalItems"], "facets": window.facets(params)}))
`
	command := exec.Command(python, "-c", script)
	command.Dir = backend
	command.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	require.NoError(t, err, stderr.String())
	var expected map[string]any
	require.NoError(t, json.Unmarshal(output, &expected), string(output))
	ids := make([]any, 0)
	for _, item := range tracePageIntegrationItems(t, result) {
		ids = append(ids, item["traceId"])
	}
	require.Equal(t, expected["ids"], ids, "storage selection must match NativeTraceWindow")
	require.Equal(t, expected["total"], result["meta"].(map[string]any)["totalItems"])
	require.Equal(t, expected["facets"], result["facets"], "storage facets must match NativeTraceWindow")
}
