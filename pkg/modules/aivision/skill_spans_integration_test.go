package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/telemetrystore/oceanbasetelemetrystore"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/stretchr/testify/require"
)

// Exercise the production handler and SQL in a uniquely named copy of the
// traces table. The existing schema and telemetry are never modified.
func TestOceanBaseSkillSpans(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for a real OceanBase integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "sn_skill_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
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
	counter := &tracePageIntegrationStore{TelemetryStore: backend}
	h := NewHandler(NewModule(settings, counter, nil, cfg))
	lastWire := ""
	fixture := tracePageIntegrationFixture{
		db: db, table: table, org: valuer.GenerateUUID().StringValue(),
		start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), lastWire: &lastWire,
	}
	query := func(t *testing.T, scope string, extra url.Values) map[string]any {
		t.Helper()
		status, payload := fixture.call(t, h.SkillSpans, scope, extra)
		require.Equal(t, http.StatusOK, status, payload)
		return payload["data"].(map[string]any)
	}

	t.Run("native_name_without_argument_path", func(t *testing.T) {
		span, _ := fixture.pair("native-name", 1)
		span.attributes = map[string]any{"agentic.skill.name": "deploy"}
		fixture.insert(t, []tracePageIntegrationSpan{span})
		result := query(t, "native-name", nil)
		require.EqualValues(t, 1, result["spanMeta"].(map[string]any)["totalItems"])
		spans := result["spans"].([]any)
		require.Len(t, spans, 1)
		require.Equal(t, "deploy", spans[0].(map[string]any)["attributes"].(map[string]any)["agentic.skill.name"])
	})

	t.Run("argument_shapes_match_previous_predicate", func(t *testing.T) {
		previous := "(" +
			traceAttributeTextSQL("agentic.tool.type") + " = 'skill' OR " +
			"COALESCE(" + traceAttributeTextSQL("agentic.skill.name") + ", '') != '' OR " +
			"POSITION('SKILL.md' IN " + traceAttributeTextSQL("agentic.tool.call.arguments") + ") > 0 OR " +
			"POSITION('SKILL.md' IN " + traceAttributeTextSQL("gen_ai.tool.call.arguments") + ") > 0 OR " +
			"POSITION('SKILL.md' IN " + traceAttributeTextSQL("gen_ai.tool.call.input") + ") > 0)"
		attributes := []map[string]any{
			{},
			{"agentic.tool.type": "skill"},
			{"agentic.skill.name": "deploy"},
			{"agentic.tool.call.arguments": `{"cmd":"cat \"skills/deploy/SKILL.md\""}`},
			{"gen_ai.tool.call.arguments": map[string]any{"path": "skills/deploy/SKILL.md"}},
			{"gen_ai.tool.call.input": []any{map[string]any{"path": "skills/deploy/SKILL.md"}}},
			{"agentic.tool.call.arguments": nil, "gen_ai.tool.call.arguments": "SKILL.md"},
			{"agentic.tool.call.arguments": "SKILL.", "gen_ai.tool.call.input": "md"},
			{"agentic.tool.call.arguments": "skill.md"},
			{"unrelated": "SKILL.md"},
			{"agentic.tool.call.arguments": map[string]any{"SKILL.md": "path"}},
			{"gen_ai.tool.call.input": `\u0053KILL.md`},
		}
		for index, attrs := range attributes {
			span, _ := fixture.pair("argument-shapes", index)
			span.attributes = attrs
			fixture.insert(t, []tracePageIntegrationSpan{span})
		}
		rows, err := db.QueryContext(ctx, "SELECT COALESCE("+previous+", FALSE),COALESCE("+skillSpanPredicateSQL()+", FALSE) FROM "+table+" WHERE space_id = ?", "argument-shapes")
		require.NoError(t, err)
		defer rows.Close()
		count := 0
		for rows.Next() {
			var before, after bool
			require.NoError(t, rows.Scan(&before, &after))
			require.Equal(t, before, after)
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, len(attributes), count)
	})

	t.Run("stable_tied_timestamp_pages", func(t *testing.T) {
		const count, limit = 501, 200
		spans := make([]tracePageIntegrationSpan, 0, count)
		for index := 0; index < count; index++ {
			span, _ := fixture.pair("tied-pages", index)
			span.traceID = fmt.Sprintf("%032x", index/2)
			span.attributes = map[string]any{"agentic.tool.type": "skill", "agentic.tool.name": "deploy"}
			spans = append(spans, span)
		}
		fixture.insert(t, spans)
		seen := map[string]bool{}
		for page := 1; page <= 3; page++ {
			result := query(t, "tied-pages", url.Values{"page": {strconv.Itoa(page)}, "limit": {strconv.Itoa(limit)}})
			meta := result["spanMeta"].(map[string]any)
			require.EqualValues(t, count, meta["totalItems"])
			require.EqualValues(t, 3, meta["totalPages"])
			for _, value := range result["spans"].([]any) {
				span := value.(map[string]any)
				identity := span["trace_id"].(string) + "/" + span["span_id"].(string)
				require.False(t, seen[identity], "span repeated across pages")
				seen[identity] = true
			}
		}
		require.Len(t, seen, count)
	})

	t.Run("empty_and_out_of_range_pages", func(t *testing.T) {
		before := counter.queries
		result := query(t, "empty", nil)
		require.Equal(t, 1, counter.queries-before)
		require.Empty(t, result["spans"])
		require.EqualValues(t, 0, result["spanMeta"].(map[string]any)["totalItems"])

		before = counter.queries
		result = query(t, "tied-pages", url.Values{"page": {"4"}, "limit": {"200"}})
		require.Equal(t, 2, counter.queries-before)
		require.Empty(t, result["spans"])
		require.EqualValues(t, 501, result["spanMeta"].(map[string]any)["totalItems"])
	})

	t.Run("mcp_spans_only_matches_full_response", func(t *testing.T) {
		spans := make([]tracePageIntegrationSpan, 0, 501)
		for index := 0; index < 501; index++ {
			span, _ := fixture.pair("mcp-pages", index)
			span.attributes = map[string]any{
				"agentic.tool.type": "mcp", "agentic.tool.raw_name": "mcp__memory__search",
			}
			spans = append(spans, span)
		}
		fixture.insert(t, spans)
		seen := map[string]bool{}
		for page := 1; page <= 3; page++ {
			extra := url.Values{"page": {strconv.Itoa(page)}, "limit": {"200"}}
			status, full := fixture.call(t, h.McpTools, "mcp-pages", extra)
			require.Equal(t, http.StatusOK, status, full)
			fullResult := full["data"].(map[string]any)

			extra.Set("spans_only", "true")
			before := counter.queries
			status, light := fixture.call(t, h.McpTools, "mcp-pages", extra)
			require.Equal(t, http.StatusOK, status, light)
			require.Equal(t, 1, counter.queries-before)
			result := light["data"].(map[string]any)
			require.Empty(t, result["data"])
			require.Equal(t, fullResult["spans"], result["spans"])
			require.Equal(t, fullResult["spanMeta"], result["spanMeta"])
			require.EqualValues(t, 501, result["spanMeta"].(map[string]any)["totalItems"])
			for _, value := range result["spans"].([]any) {
				span := value.(map[string]any)
				identity := span["trace_id"].(string) + "/" + span["span_id"].(string)
				require.False(t, seen[identity])
				seen[identity] = true
			}
		}
		require.Len(t, seen, 501)
	})
}
