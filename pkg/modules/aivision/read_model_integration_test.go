package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/telemetrystore/oceanbasetelemetrystore"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// This test writes only newly named tables in the explicitly provided test DB.
func TestOceanBaseTraceReadModel(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for isolated real SQL validation")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "rm_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	router := mux.NewRouter()
	org := valuer.GenerateUUID().StringValue()
	key := "isolated-test-key"
	require.NoError(t, ingest.AddToRouter(router, ingest.Config{DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: key}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		for _, suffix := range []string{"traces", "logs", "metric_samples", "trace_sources", "trace_projections", "trace_projection_coverage", "projection_maintenance"} {
			_, err := db.Exec("DROP TABLE IF EXISTS `" + prefix + "_" + suffix + "`")
			require.NoError(t, err)
		}
	})
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: prefix}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	fixture := tracePageIntegrationFixture{db: db, table: store.tracesTable(), org: org, start: start}
	spans := []tracePageIntegrationSpan{}
	for i := 1; i <= 3; i++ {
		root, child := fixture.pair("scope", i)
		root.start += int64(i * 100)
		child.start += int64(i * 100)
		spans = append(spans, root, child)
	}
	fixture.insert(t, spans)
	for i := 0; i < 3; i++ {
		result, err := store.backfillTraceSources(ctx, org, "scope", 1)
		require.NoError(t, err)
		require.Equal(t, false, result.(map[string]any)["complete"])
	}
	result, err := store.backfillTraceSources(ctx, org, "scope", 1)
	require.NoError(t, err)
	require.Equal(t, true, result.(map[string]any)["complete"])
	contextHash := strings.Repeat("a", 64)
	dirty, err := store.dirtyTraceSources(ctx, org, "scope", contextHash, "", 10, "")
	require.NoError(t, err)
	require.Len(t, dirty.(map[string]any)["data"], 3)
	for i := 1; i <= 3; i++ {
		trace := fmt.Sprintf("%032x", i)
		snapshot, err := store.traceSourceSnapshot(ctx, org, "scope", trace)
		require.NoError(t, err)
		require.EqualValues(t, 2, snapshot.(map[string]any)["span_count"])
		row, _ := json.Marshal(map[string]any{"trace_id": trace, "start_time_unix_nano": fmt.Sprint(start.UnixNano() + int64(i*100)), "end_time_unix_nano": fmt.Sprint(start.Add(time.Second).UnixNano()), "duration_ms": 1000, "status_code": 1, "span_count": 2, "total_tokens": int64(9007199254740993)})
		duration := 1000.0
		env := []string{"", "Prod", "prod", "Prod"}[i]
		username := ""
		var user *string
		if i == 2 {
			user = &username
		}
		if i == 3 {
			username = "sdk-user"
			user = &username
		}
		p := traceProjection{MaintenanceGeneration: integrationProjectionGeneration(t, db, prefix), ProjectionContext: contextHash, SpaceID: "scope", TraceID: trace, SourceRevision: 1, SemanticsVersion: 1, AgentUUID: "agent", AgentProduct: "Codex", UserID: "subject", Period: start.UnixMilli(), Duration: &duration, Status: 1, NodeCount: 2, Environment: &env, Username: user, SceneValues: []string{"dev", "dev", "eval"}, Tags: []string{[]string{"", "alpha", "beta", "gamma"}[i]}, InputText: fmt.Sprintf("i\u0307_%% literal trace-%d", i), OutputText: "answer", Row: row, Preview: validProjection().Preview}
		require.NoError(t, validateTraceProjection(p))
		published, err := store.publishTraceProjection(ctx, org, p)
		require.NoError(t, err)
		require.Equal(t, true, published.(map[string]any)["published"])
		p.SourceRevision = 2
		stale, err := store.publishTraceProjection(ctx, org, p)
		require.NoError(t, err)
		require.Equal(t, readModelUnsupported("source_revision_changed"), stale)
	}
	params := nativeListParams{ProjectionContext: contextHash, From: start.UnixMilli(), To: start.Add(time.Minute).UnixMilli(), SpaceID: "scope", User: "subject", AgentProduct: "Codex", AgentUUID: "agent", Page: 1, Limit: 10, OrderBy: "start_time_unix_nano.desc"}
	page, err := store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	response := page.(map[string]any)
	require.Equal(t, true, response["supported"])
	rows := response["data"].([]map[string]any)
	require.Len(t, rows, 3)
	require.Equal(t, fmt.Sprintf("%032x", 3), rows[0]["trace_id"])
	require.Equal(t, json.Number("9007199254740993"), rows[0]["total_tokens"])
	// A new optional metrics supplement must not invalidate ready list rows.
	legacyDirty, err := store.dirtyTraceSources(ctx, org, "scope", contextHash, "", 10, "")
	require.NoError(t, err)
	require.Empty(t, legacyDirty.(map[string]any)["data"])
	upgradeDirty, err := store.dirtyTraceSources(ctx, org, "scope", contextHash, "", 10, traceMetricsSupplementVersion)
	require.NoError(t, err)
	require.Len(t, upgradeDirty.(map[string]any)["data"], 3)
	require.Equal(t, traceMetricsSupplementVersion, upgradeDirty.(map[string]any)["required_supplement"])
	// Only this test's isolated projection fixture changes. Raw Spans and
	// source revisions remain untouched during a gradual supplement upgrade.
	_, err = db.Exec("UPDATE "+store.readModelTable("trace_projections")+" SET row_json=JSON_SET(row_json,'$._metrics',JSON_OBJECT('version',?)) WHERE org_id=? AND space_id=? AND trace_id=?",
		traceMetricsSupplementVersion, org, "scope", fmt.Sprintf("%032x", 1))
	require.NoError(t, err)
	upgradeDirty, err = store.dirtyTraceSources(ctx, org, "scope", contextHash, "", 10, traceMetricsSupplementVersion)
	require.NoError(t, err)
	require.Len(t, upgradeDirty.(map[string]any)["data"], 2)
	upgradePage, err := store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, true, upgradePage.(map[string]any)["supported"])
	require.Len(t, upgradePage.(map[string]any)["data"], 3)
	var cleanSources, originalRevisions int
	require.NoError(t, db.QueryRow("SELECT SUM(NOT dirty),SUM(source_revision=1) FROM "+store.readModelTable("trace_sources")+" WHERE org_id=? AND space_id=?", org, "scope").Scan(&cleanSources, &originalRevisions))
	require.Equal(t, 3, cleanSources)
	require.Equal(t, 3, originalRevisions)
	facets := response["facets"].(nativeFacets)
	require.Len(t, facets["env"], 2)
	require.Len(t, facets["scene"], 2)
	for _, value := range facets["scene"] {
		require.Equal(t, 3, value.Count)
	}
	filtered := params
	filtered.Keyword = "trace-3"
	filtered.SearchType = "input"
	filtered.Filters = []nativeFilter{{Field: "userId", Op: "=", Value: "sdk-user"}, {Field: "duration_ms", Op: ">=", Value: json.Number("999")}}
	filteredPage, err := store.readModelPage(ctx, org, filtered)
	require.NoError(t, err)
	filteredResult := filteredPage.(map[string]any)
	filteredMeta := filteredResult["meta"].(map[string]any)
	require.Equal(t, 1, filteredMeta["totalItems"])
	require.Equal(t, 3, filteredMeta["windowTotalItems"], "independent facet total preserves the unfiltered authorized Agent window")
	filteredFacets := filteredResult["facets"].(nativeFacets)
	require.Equal(t, []nativeFacetValue{{Value: "sdk-user", Count: 1}}, filteredFacets["username"])
	require.Equal(t, []nativeFacetValue{{Value: "gamma", Count: 1}}, filteredFacets["tags"])
	require.Equal(t, 3, response["meta"].(map[string]any)["windowTotalItems"])
	filtered.ProjectionContext = strings.Repeat("b", 64)
	staleWindow, err := store.readModelPage(ctx, org, filtered)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_coverage_stale"), staleWindow)
	_, err = db.Exec("UPDATE "+store.readModelTable("trace_projections")+" SET duration=NULL,end_ns=NULL WHERE org_id=? AND space_id='scope' AND trace_id=?", org, fmt.Sprintf("%032x", 1))
	require.NoError(t, err)
	params.OrderBy = "duration_ms.desc"
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%032x", 1), page.(map[string]any)["data"].([]map[string]any)[0]["trace_id"])
	params.OrderBy = "duration_ms.asc"
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%032x", 1), page.(map[string]any)["data"].([]map[string]any)[2]["trace_id"])
	params.OrderBy = "end_time_unix_nano.desc"
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%032x", 1), page.(map[string]any)["data"].([]map[string]any)[0]["trace_id"])
	params.OrderBy = "start_time_unix_nano.desc"
	params.Tags = []string{"alpha", "beta"}
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Len(t, page.(map[string]any)["data"], 2)
	params.Tags = nil
	params.Keyword = "%"
	params.SearchType = "input"
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Len(t, page.(map[string]any)["data"], 3)
	params.Keyword = ""
	params.Filters = []nativeFilter{{Field: "userId", Op: "IS_NULL"}}
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Len(t, page.(map[string]any)["data"], 1)
	params.Filters = []nativeFilter{{Field: "userId", Op: "=", Value: ""}}
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Len(t, page.(map[string]any)["data"], 1)
	params.Filters = nil
	params.AgentUUID = "other"
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Len(t, page.(map[string]any)["data"], 0)
	params.AgentUUID = "agent"
	params.ProjectionContext = strings.Repeat("b", 64)
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_coverage_stale"), page)
	params.ProjectionContext = contextHash
	// EXPLAIN verifies scope/Agent candidate index on the typed table.
	where, args, err := readModelWhere(org, params)
	require.NoError(t, err)
	explain, err := db.QueryContext(ctx, "EXPLAIN SELECT p.trace_id FROM "+store.readModelTable("trace_projections")+" p WHERE "+where+" ORDER BY p.start_ns DESC LIMIT 10", args...)
	require.NoError(t, err)
	columns, err := explain.Columns()
	require.NoError(t, err)
	plans := []map[string]any{}
	for explain.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		require.NoError(t, explain.Scan(ptrs...))
		plan := map[string]any{}
		for i, c := range columns {
			v := values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			plan[c] = v
		}
		plans = append(plans, plan)
	}
	require.NoError(t, explain.Close())
	planJSON, _ := json.Marshal(plans)
	t.Logf("EXPLAIN typed summary candidates: %s", planJSON)
	require.NotEmpty(t, plans)
	t.Run("batch_publication_commits_bounded_chunks_with_per_trace_CAS", func(t *testing.T) {
		const batchScope = "batch-publication"
		var spans []tracePageIntegrationSpan
		for i := 1; i <= 26; i++ {
			root, child := fixture.pair(batchScope, i)
			spans = append(spans, root, child)
		}
		fixture.insert(t, spans)
		enrolled, err := store.backfillTraceSources(ctx, org, batchScope, 500)
		require.NoError(t, err)
		require.EqualValues(t, 26, enrolled.(map[string]any)["enrolled"])
		items := make([]traceProjection, 0, 26)
		for i := 1; i <= 26; i++ {
			p := validProjection()
			p.MaintenanceGeneration = integrationProjectionGeneration(t, db, prefix)
			p.SpaceID, p.TraceID = batchScope, fmt.Sprintf("%032x", i)
			p.UserID, p.AgentProduct, p.Period = "subject", "Codex", start.UnixMilli()
			p.Row, err = json.Marshal(map[string]any{"trace_id": p.TraceID, "start_time_unix_nano": fmt.Sprint(start.UnixNano())})
			require.NoError(t, err)
			items = append(items, p)
		}
		items[25].SourceRevision = 2 // The real source remains revision1 and dirty.
		published, err := store.publishTraceBatch(ctx, org, items)
		require.NoError(t, err)
		data := published.(map[string]any)["data"].([]any)
		require.Len(t, data, 26)
		for i, item := range items {
			r := data[i].(map[string]any)
			require.Equal(t, item.TraceID, r["trace_id"])
			if i < 25 {
				require.Equal(t, true, r["published"])
			} else {
				require.Equal(t, "source_revision_changed", r["reason"])
			}
		}
		var projectionCount, clean, revisionOne int
		require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+store.readModelTable("trace_projections")+" WHERE org_id=? AND space_id=?", org, batchScope).Scan(&projectionCount))
		require.Equal(t, 25, projectionCount)
		require.NoError(t, db.QueryRow("SELECT SUM(NOT dirty),SUM(source_revision=1) FROM "+store.readModelTable("trace_sources")+" WHERE org_id=? AND space_id=?", org, batchScope).Scan(&clean, &revisionOne))
		require.Equal(t, 25, clean)
		require.Equal(t, 26, revisionOne)
		foreign, err := store.publishTraceBatch(ctx, "other-org", items[:1])
		require.NoError(t, err)
		require.Equal(t, "source_not_enrolled", foreign.(map[string]any)["data"].([]any)[0].(map[string]any)["reason"])
		require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+store.readModelTable("trace_projections")+" WHERE org_id=?", "other-org").Scan(&projectionCount))
		require.Zero(t, projectionCount)
	})

	// Correct both previously stored spans via the real production ingest path.
	trace := fmt.Sprintf("%032x", 1)
	newStart := start.Add(5 * time.Minute).UnixNano()
	body := fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"aivision.space.id","value":{"stringValue":"scope"}},{"key":"aivision.user.id","value":{"stringValue":"subject"}},{"key":"aivision.agent.id","value":{"stringValue":"agent"}},{"key":"agent.product","value":{"stringValue":"Codex"}}]},"scopeSpans":[{"spans":[{"traceId":"%s","spanId":"%016x","name":"corrected-root","startTimeUnixNano":"%d","endTimeUnixNano":"%d"},{"traceId":"%s","spanId":"%016x","parentSpanId":"%016x","name":"corrected-child","startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, trace, 3, newStart, newStart+1, trace, 4, 3, newStart+1, newStart+2)
	request := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Signoz-Ingest-Key", key)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	var revision, min, max int64
	require.NoError(t, db.QueryRow("SELECT source_revision,min_timestamp,max_timestamp FROM "+store.readModelTable("trace_sources")+" WHERE org_id=? AND space_id='scope' AND trace_id=?", org, trace).Scan(&revision, &min, &max))
	require.EqualValues(t, 2, revision)
	require.Equal(t, newStart, min)
	require.Equal(t, newStart+1, max)
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("boundary_crossing_trace"), page)
	// Source moved completely out, but old projection is in-window: cannot serve stale rows.
	var indexed int64
	require.NoError(t, db.QueryRow("SELECT MIN(timestamp) FROM "+store.tracesTable()+" WHERE org_id=? AND space_id='scope' AND trace_id=?", org, trace).Scan(&indexed))
	require.Equal(t, newStart, indexed)
	// Ordinary restarts preserve trustworthy coverage and existing dirty revisions.
	initializeTelemetryStore := func(tracking string) {
		t.Helper()
		t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", tracking)
		t.Setenv("SIGNOZ_INGEST_INTERNAL_KEY", "")
		cfg := telemetrystore.NewConfigFactory().New().(telemetrystore.Config)
		cfg.Provider = "oceanbase"
		cfg.OceanBase.DSN = dsn
		cfg.OceanBase.TablePrefix = prefix
		provider, err := oceanbasetelemetrystore.New(ctx, instrumentationtest.New().ToProviderSettings(), cfg)
		require.NoError(t, err)
		require.NoError(t, provider.(interface{ Close() error }).Close())
	}
	initializeTelemetryStore("true")
	reactivated := mux.NewRouter()
	cfg := ingest.Config{DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: ""}
	require.NoError(t, ingest.AddToRouter(reactivated, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))))
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("boundary_crossing_trace"), page)
	_, err = store.backfillTraceSources(ctx, org, "scope", 10)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT source_revision FROM "+store.readModelTable("trace_sources")+" WHERE org_id=? AND space_id='scope' AND trace_id=?", org, trace).Scan(&revision))
	require.EqualValues(t, 2, revision)
	// Full rollback remains functional even if every derived table is absent.
	for _, suffix := range []string{"trace_sources", "trace_projections", "trace_projection_coverage"} {
		_, err := db.Exec("DROP TABLE " + store.readModelTable(suffix))
		require.NoError(t, err)
	}
	initializeTelemetryStore("false")
	var derivedTables int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", prefix+"_trace_sources").Scan(&derivedTables))
	require.Zero(t, derivedTables, "tracking-disabled provider startup must not recreate derived schema")
	legacy := mux.NewRouter()
	cfg.InternalKey = key
	cfg.ProjectionMaintenanceDisabled = true
	require.NoError(t, ingest.AddToRouter(legacy, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))))
	rollbackRequest := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(body))
	rollbackRequest.Header.Set("Content-Type", "application/json")
	rollbackRequest.Header.Set("X-Signoz-Ingest-Key", key)
	rollbackRecorder := httptest.NewRecorder()
	legacy.ServeHTTP(rollbackRecorder, rollbackRequest)
	require.Equal(t, 200, rollbackRecorder.Code, rollbackRecorder.Body.String())
	// Default-on replicas cannot undo the fleet-wide OFF latch.
	cfg.ProjectionMaintenanceDisabled = false
	cfg.InternalKey = ""
	initializeTelemetryStore("true")
	require.NoError(t, ingest.AddToRouter(mux.NewRouter(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil))))
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_maintenance_unavailable"), page)
	// Explicit resume invalidates only derived rows and enrolls history afresh.
	require.NoError(t, ingest.ResumeProjectionMaintenance(ctx, db, prefix))
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_summary_columns_unavailable"), page)
	// A direct maintenance caller also completes the additive helper migration;
	// the public resume route/provider perform this initialization automatically.
	require.NoError(t, ingest.InitializeTraceProjectionSummaries(ctx, db, prefix))
	page, err = store.readModelPage(ctx, org, params)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("historical_coverage_incomplete"), page)

}

func TestOceanBaseTraceReadModelScopeRetag(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for isolated real SQL validation")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "rm_scope_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:10]
	org := valuer.GenerateUUID().StringValue()
	key := "isolated-retag-key"
	router := mux.NewRouter()
	require.NoError(t, ingest.AddToRouter(router, ingest.Config{DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: key}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		for _, suffix := range []string{"traces", "logs", "metric_samples", "trace_sources", "trace_projections", "trace_projection_coverage", "projection_maintenance"} {
			_, err := db.Exec("DROP TABLE IF EXISTS `" + prefix + "_" + suffix + "`")
			require.NoError(t, err)
		}
	})
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: prefix}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	fixture := tracePageIntegrationFixture{db: db, table: store.tracesTable(), org: org, start: start}
	contextHash := strings.Repeat("a", 64)
	for _, tc := range []struct{ name, user, product string }{{"user_only", "other-user", "Codex"}, {"product_only", "subject", "OtherProduct"}, {"both", "other-user", "OtherProduct"}} {
		t.Run(tc.name, func(t *testing.T) {
			scope := "retag-" + tc.name
			root, child := fixture.pair(scope, 1)
			fixture.insert(t, []tracePageIntegrationSpan{root, child})
			_, err := store.backfillTraceSources(ctx, org, scope, 10)
			require.NoError(t, err)
			row, _ := json.Marshal(map[string]any{"trace_id": root.traceID, "start_time_unix_nano": fmt.Sprint(start.UnixNano()), "status_code": 1, "span_count": 2})
			p := traceProjection{MaintenanceGeneration: integrationProjectionGeneration(t, db, prefix), ProjectionContext: contextHash, SpaceID: scope, TraceID: root.traceID, SourceRevision: 1, SemanticsVersion: 1, AgentUUID: "agent", AgentProduct: "Codex", UserID: "subject", Period: start.UnixMilli(), Status: 1, NodeCount: 2, Row: row, Preview: validProjection().Preview}
			require.NoError(t, validateTraceProjection(p))
			published, err := store.publishTraceProjection(ctx, org, p)
			require.NoError(t, err)
			require.Equal(t, true, published.(map[string]any)["published"])
			oldQuery := nativeListParams{ProjectionContext: contextHash, From: start.UnixMilli(), To: start.Add(time.Minute).UnixMilli(), SpaceID: scope, User: "subject", AgentProduct: "Codex", AgentUUID: "agent", Page: 1, Limit: 10}
			page, err := store.readModelPage(ctx, org, oldQuery)
			require.NoError(t, err)
			require.Equal(t, true, page.(map[string]any)["supported"])
			body := fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"aivision.space.id","value":{"stringValue":"%s"}},{"key":"aivision.user.id","value":{"stringValue":"%s"}},{"key":"aivision.agent.id","value":{"stringValue":"agent"}},{"key":"agent.product","value":{"stringValue":"%s"}}]},"scopeSpans":[{"spans":[{"traceId":"%s","spanId":"%s","name":"retag-root","startTimeUnixNano":"%d","endTimeUnixNano":"%d"},{"traceId":"%s","spanId":"%s","parentSpanId":"%s","name":"retag-child","startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, scope, tc.user, tc.product, root.traceID, root.spanID, root.start, root.end, child.traceID, child.spanID, root.spanID, child.start, child.end)
			request := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Signoz-Ingest-Key", key)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			require.Equal(t, 200, recorder.Code, recorder.Body.String())
			// A stale projection under the prior authorization scope must invalidate it,
			// even though its current source now belongs to the corrected scope.
			page, err = store.readModelPage(ctx, org, oldQuery)
			require.NoError(t, err)
			require.Equal(t, readModelUnsupported("projection_coverage_stale"), page, "old scope must not return the stale canonical row")
			newQuery := oldQuery
			newQuery.User = tc.user
			newQuery.AgentProduct = tc.product
			page, err = store.readModelPage(ctx, org, newQuery)
			require.NoError(t, err)
			require.Equal(t, readModelUnsupported("projection_coverage_stale"), page, "new scope must not report false empty while its projection is pending")
			p.SourceRevision = 2
			p.UserID = tc.user
			p.AgentProduct = tc.product
			published, err = store.publishTraceProjection(ctx, org, p)
			require.NoError(t, err)
			require.Equal(t, true, published.(map[string]any)["published"])
			page, err = store.readModelPage(ctx, org, oldQuery)
			require.NoError(t, err)
			require.Equal(t, true, page.(map[string]any)["supported"])
			require.Empty(t, page.(map[string]any)["data"])
			page, err = store.readModelPage(ctx, org, newQuery)
			require.NoError(t, err)
			require.Equal(t, true, page.(map[string]any)["supported"])
			require.Len(t, page.(map[string]any)["data"], 1)
		})
	}
}
