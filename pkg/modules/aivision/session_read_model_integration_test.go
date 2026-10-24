package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestOceanBaseCanonicalSessionReadModel(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated test DSN for real SQL validation")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	prefix := "session_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	org := valuer.GenerateUUID().StringValue()
	require.NoError(t, ingest.AddToRouter(mux.NewRouter(), ingest.Config{TraceReadModelTracking: true, DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: "isolated-session-test"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		for _, suffix := range []string{"traces", "logs", "metric_samples", "trace_sources", "trace_projections", "trace_projection_coverage"} {
			_, e := db.Exec("DROP TABLE IF EXISTS `" + prefix + "_" + suffix + "`")
			require.NoError(t, e)
		}
	})
	store := &oceanBaseStore{db: db, readModelEnabled: true, authoritativeIngest: true, cfg: config{TablePrefix: prefix}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	fixture := tracePageIntegrationFixture{db: db, table: store.tracesTable(), org: org, start: start}
	spans := []tracePageIntegrationSpan{}
	for i := 1; i <= 7; i++ {
		root, child := fixture.pair("scope", i)
		spans = append(spans, root, child)
	}
	fixture.insert(t, spans)
	for {
		result, e := store.backfillTraceSources(ctx, org, "scope", 100)
		require.NoError(t, e)
		if result.(map[string]any)["complete"] == true {
			break
		}
	}
	contextHash := strings.Repeat("a", 64)
	for i := 1; i <= 7; i++ {
		trace := fmt.Sprintf("%032x", i)
		sid := "session-one"
		if i == 4 {
			sid = "session-two"
		}
		if i == 5 {
			sid = "session-unknown"
		}
		if i == 6 {
			sid = "session-tool-only"
		}
		if i == 7 {
			sid = "session-one"
		}
		user, tag := "alice", "alpha"
		if i == 2 {
			user, tag = "bob", "beta"
		}
		status := 1
		if i == 2 {
			status = 2
		}
		if i == 5 {
			status = 0
		}
		var tokens any = int64(i * 10)
		if i == 4 {
			tokens = int64(9007199254740993)
		}
		billable := 1
		if i == 5 {
			tokens = nil
		}
		if i == 3 || i == 6 {
			tokens = nil
			billable = 0
		}
		startNS := start.Add(time.Duration(i) * time.Second).UnixNano()
		endNS := startNS + int64(time.Second)
		fullInput := strings.Repeat("canonical root input ", 5000) + fmt.Sprintf("CANONICAL_COMPLETE_SENTINEL_%d", i)
		aliasIO := "SDK_FULL_IO_SENTINEL_" + strings.Repeat("duplicated SDK IO ", 5000)
		row, _ := json.Marshal(map[string]any{"trace_id": trace, "session_id": sid, "start_time_unix_nano": fmt.Sprint(startNS), "end_time_unix_nano": fmt.Sprint(endNS), "usage": map[string]any{"total": tokens}, "coverage": map[string]any{"billableCalls": billable}, "attributes.gen_ai.input.messages": fullInput, "attributes.gen_ai.output.messages": "answer", "attributes.langfuse.trace.input": aliasIO, "attributes.langfuse.trace.output": aliasIO, "observation.input": map[string]any{"messages": []any{aliasIO}}, "observation.output": aliasIO, "attributes.eino.input": aliasIO})
		env := "prod"
		duration := 1000.0
		physical := "subject"
		if i == 7 {
			physical = "other"
			_, e := db.Exec("UPDATE "+store.readModelTable("trace_sources")+" SET user_id=? WHERE org_id=? AND space_id='scope' AND trace_id=?", physical, org, trace)
			require.NoError(t, e)
		}
		projection := traceProjection{ProjectionContext: contextHash, SpaceID: "scope", TraceID: trace, SourceRevision: 1, SemanticsVersion: 1, AgentUUID: "agent", AgentProduct: "Codex", UserID: physical, Period: startNS / 1000000, Duration: &duration, Status: status, NodeCount: 2, Environment: &env, Username: &user, SessionID: sid, Tags: []string{tag}, SceneValues: []string{}, InputText: fmt.Sprintf("needle %%_中文 turn-%d ", i) + strings.ToLower(fullInput), OutputText: "answer", Row: row, Preview: validProjection().Preview}
		result, e := store.publishTraceProjection(ctx, org, projection)
		require.NoError(t, e)
		require.Equal(t, true, result.(map[string]any)["published"])
	}
	p := sessionReadModelParams{nativeListParams: nativeListParams{ProjectionContext: contextHash, From: start.UnixMilli(), To: start.Add(time.Minute).UnixMilli(), SpaceID: "scope", User: "subject", AgentProduct: "Codex", AgentUUID: "agent", Page: 1, Limit: 20}}
	get := func(p sessionReadModelParams) map[string]any {
		store.sessionPageBatchEnabled = false
		store.sessionTokenQueryOptimized = false
		original, e := store.sessionReadModelPage(ctx, org, p)
		require.NoError(t, e)
		store.sessionTokenQueryOptimized = true
		optimized, e := store.sessionReadModelPage(ctx, org, p)
		require.NoError(t, e)
		before, after := original.(map[string]any), optimized.(map[string]any)
		require.Equal(t, true, after["supported"])
		beforeMeta, afterMeta := before["meta"].(map[string]any), after["meta"].(map[string]any)
		require.Equal(t, "original", beforeMeta["sessionTokenQueryStrategy"])
		strategy := "original"
		if p.MinTotalTokens != nil {
			strategy = "json_table"
		}
		require.Equal(t, strategy, afterMeta["sessionTokenQueryStrategy"])
		delete(beforeMeta, "sessionTokenQueryStrategy")
		delete(afterMeta, "sessionTokenQueryStrategy")
		require.Equal(t, before, after, "same fixture must preserve full response, facets and all other metadata")
		store.sessionPageBatchEnabled = true
		batched, e := store.sessionReadModelPage(ctx, org, p)
		require.NoError(t, e)
		batch := batched.(map[string]any)
		batchMeta := batch["meta"].(map[string]any)
		require.Equal(t, "individual", afterMeta["sessionPageSummaryStrategy"])
		require.Equal(t, "batch", batchMeta["sessionPageSummaryStrategy"])
		delete(batchMeta, "sessionTokenQueryStrategy")
		delete(batchMeta, "sessionPageSummaryStrategy")
		delete(afterMeta, "sessionPageSummaryStrategy")
		require.Equal(t, after, batch, "batched summaries preserve complete canonical data, matched turns, facets, ordering and pagination")
		store.sessionPageBatchEnabled = false
		return after
	}
	result := get(p)
	require.Equal(t, 4, result["meta"].(map[string]any)["totalItems"])
	for _, group := range result["data"].([]map[string]any) {
		if group["session_id"] == "session-two" {
			usage := group["traces"].([]map[string]any)[0]["usage"].(map[string]any)
			require.Equal(t, json.Number("9007199254740993"), usage["total"], "storage summary whitelist preserves usage integers above JavaScript precision")
		}
	}
	p.Tags = []string{"beta"}
	result = get(p)
	groups := result["data"].([]map[string]any)
	require.Len(t, groups, 1)
	require.EqualValues(t, 3, groups[0]["traceCount"])
	require.Len(t, groups[0]["traces"], 3)
	require.Equal(t, []string{fmt.Sprintf("%032x", 2)}, groups[0]["matchedTraceIds"])
	row := groups[0]["traces"].([]map[string]any)[0]
	require.Equal(t, row["ioPreview"].(map[string]any)["input"], row["attributes.gen_ai.input.messages"], "full root IO must not leave indexed session pages")
	for _, key := range []string{"attributes.langfuse.trace.input", "attributes.langfuse.trace.output", "observation.input", "observation.output", "attributes.eino.input"} {
		require.NotContains(t, row, key, "raw aliases must be absent from the storage summary whitelist")
	}
	bounded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Less(t, len(bounded), 50000)
	require.NotContains(t, string(bounded), "SDK_FULL_IO_SENTINEL_")
	p.Username = "alice"
	result = get(p)
	require.Empty(t, result["data"], "user and tag must match same Trace, not different turns")
	p.Username = ""
	p.Tags = nil
	p.Keyword = "canonical_complete_sentinel_2"
	p.SearchType = "input"
	result = get(p)
	require.Equal(t, 1, result["meta"].(map[string]any)["totalItems"], "complete input text remains searchable beyond the bounded preview")
	require.Len(t, result["data"].([]map[string]any)[0]["traces"], 3)
	p.Keyword = "%_中文"
	p.SearchType = "input"
	result = get(p)
	require.Equal(t, 4, result["meta"].(map[string]any)["totalItems"])
	p.Keyword = ""
	p.SessionStatus = "error"
	result = get(p)
	require.Equal(t, 1, result["meta"].(map[string]any)["totalItems"])
	p.SessionStatus = ""
	minimum := int64(0)
	p.MinTotalTokens = &minimum
	result = get(p)
	require.Equal(t, 2, result["meta"].(map[string]any)["totalItems"], "unknown billable and all tool-only totals do not match reported zero")
	p.Tags = []string{"beta"}
	result = get(p)
	require.Len(t, result["data"].([]map[string]any)[0]["traces"], 3, "Token filter retains unrelated turns")
	require.Equal(t, []nativeFacetValue{{Value: "alpha", Count: 2}, {Value: "beta", Count: 1}}, result["facets"].(nativeFacets)["tags"], "tag facet excludes its own filter while retaining Token eligibility")
	p.Username = "alice"
	result = get(p)
	require.Empty(t, result["data"], "Token filtering preserves same-Trace user/tag matching")
	require.Equal(t, []nativeFacetValue{{Value: "bob", Count: 1}}, result["facets"].(nativeFacets)["username"], "username facet excludes its own filter")
	p.Username, p.Tags = "", nil
	p.Env = "missing"
	result = get(p)
	require.Empty(t, result["data"])
	require.Equal(t, []nativeFacetValue{{Value: "prod", Count: 2}}, result["facets"].(nativeFacets)["env"], "environment facet excludes its own filter")
	p.Env = ""
	p.SessionStatus = "error"
	duration := 2999.0
	p.MinDurationMS = &duration
	result = get(p)
	require.Equal(t, 1, result["meta"].(map[string]any)["totalItems"])
	duration = 3001
	require.Empty(t, get(p)["data"])
	p.SessionStatus, p.MinDurationMS = "", nil
	minimum = 9007199254740993
	result = get(p)
	require.Equal(t, "session-two", result["data"].([]map[string]any)[0]["session_id"])
	minimum++
	require.Empty(t, get(p)["data"], "threshold one above exact large integer must exclude it")
	minimum = 30
	p.MinTraceCount = &minimum
	result = get(p)
	require.Empty(t, result["data"])
	p.MinTraceCount = nil
	p.MinTotalTokens = nil
	p.SessionID = "session-one"
	result = get(p)
	require.Len(t, result["data"].([]map[string]any)[0]["traces"], 3, "same Session ID across physical user never leaks")
	p.AgentUUID = "other"
	result = get(p)
	require.Empty(t, result["data"])
	p.AgentUUID = "agent"
	_, err = db.Exec("UPDATE "+store.readModelTable("trace_sources")+" SET dirty=TRUE,user_id='new-subject' WHERE org_id=? AND space_id='scope' AND trace_id=?", org, fmt.Sprintf("%032x", 1))
	require.NoError(t, err)
	stale, e := store.sessionReadModelPage(ctx, org, p)
	require.NoError(t, e)
	require.Equal(t, readModelUnsupported("projection_coverage_stale"), stale, "old physical scope cannot serve previous session membership")
}
