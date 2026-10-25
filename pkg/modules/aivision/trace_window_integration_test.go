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

func TestOceanBaseRecoveryWindowMergesAndFallsBack(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated real SQL DSN")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	prefix := "window_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	org := valuer.GenerateUUID().StringValue()
	require.NoError(t, ingest.AddToRouter(mux.NewRouter(), ingest.Config{DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: "isolated-window-test"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		for _, suffix := range []string{"traces", "logs", "metric_samples", "trace_sources", "trace_projections", "trace_projection_coverage", "projection_maintenance"} {
			_, e := db.Exec("DROP TABLE IF EXISTS `" + prefix + "_" + suffix + "`")
			require.NoError(t, e)
		}
	})
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: prefix}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	fixture := tracePageIntegrationFixture{db: db, table: store.tracesTable(), org: org, start: start}
	spans := []tracePageIntegrationSpan{}
	for i := 1; i <= 3; i++ {
		root, child := fixture.pair("scope", i)
		if i == 3 {
			root.start -= 1
		}
		spans = append(spans, root, child)
	}
	fixture.insert(t, spans)
	_, err = store.backfillTraceSources(ctx, org, "scope", 100)
	require.NoError(t, err)
	// Only one Trace is published. History can still be incomplete, and the
	// boundary Trace must be represented by only its in-window raw child span.
	id := fmt.Sprintf("%032x", 1)
	row, _ := json.Marshal(map[string]any{"trace_id": id, "start_time_unix_nano": fmt.Sprint(start.UnixNano()), "span_count": 2, "attributes.gen_ai.input.messages": "complete clean input", "_traceTags": []string{}, "_sceneValues": []string{}})
	projection := validProjection()
	projection.MaintenanceGeneration = integrationProjectionGeneration(t, db, prefix)
	projection.SpaceID = "scope"
	projection.TraceID = id
	projection.Period = start.UnixMilli()
	projection.SourceRevision = 1
	projection.AgentUUID = "agent"
	projection.UserID = "subject"
	projection.AgentProduct = "Codex"
	projection.Row = row
	projection.NodeCount = 2
	_, err = store.publishTraceProjection(ctx, org, projection)
	require.NoError(t, err)
	p := nativeListParams{From: start.UnixMilli(), To: start.Add(time.Minute).UnixMilli(), SpaceID: "scope", User: "subject", AgentProduct: "Codex", AgentUUID: "agent", ProjectionContext: projection.ProjectionContext}
	result, err := store.traceWindow(ctx, org, p, false)
	require.NoError(t, err)
	r := result.(map[string]any)
	require.Len(t, r["rows"], 1)
	require.Len(t, r["raw"], 2)
	meta := r["meta"].(map[string]any)
	require.EqualValues(t, 3, meta["traceCount"])
	require.EqualValues(t, 5, meta["spanCount"])
	require.EqualValues(t, 3, meta["rawSpanCount"])
	require.Equal(t, "complete clean input", r["rows"].([]map[string]any)[0]["attributes.gen_ai.input.messages"])
	// A denormalized summary can exceed the transfer budget even while every
	// complete raw Span fits. The alternate representation must retain the same
	// snapshot, all Trace/Span identities and explicit raw-only metadata.
	originalRow := projection.Row
	wide := map[string]any{}
	require.NoError(t, json.Unmarshal(projection.Row, &wide))
	wide["attributes.gen_ai.input.messages"] = strings.Repeat("full canonical IO ", 4096)
	projection.Row, err = json.Marshal(wide)
	require.NoError(t, err)
	_, err = store.publishTraceProjection(ctx, org, projection)
	require.NoError(t, err)
	budgetResult, err := store.traceWindowWithPayloadLimit(ctx, org, p, false, traceWindowEnvelopeReserve+16384)
	require.NoError(t, err)
	bounded := budgetResult.(map[string]any)
	require.Empty(t, bounded["rows"])
	require.Len(t, bounded["raw"], 3)
	boundedMeta := bounded["meta"].(map[string]any)
	require.Equal(t, true, boundedMeta["raw"])
	require.Equal(t, "canonical_summary_budget", boundedMeta["recoveryReason"])
	require.EqualValues(t, 0, boundedMeta["projectedTraceCount"])
	require.EqualValues(t, 0, boundedMeta["omittedAgentTraceCount"])
	require.EqualValues(t, 5, boundedMeta["rawSpanCount"])
	explicitRaw, err := store.traceWindow(ctx, org, p, true)
	require.NoError(t, err)
	require.Equal(t, explicitRaw.(map[string]any)["raw"], bounded["raw"])
	projection.Row = originalRow
	_, err = store.publishTraceProjection(ctx, org, projection)
	require.NoError(t, err)

	// The batch endpoint must preserve every single-source field, including
	// source scope, revision, generation, precise timestamps and complete IO.
	ids := []string{fmt.Sprintf("%032x", 3), fmt.Sprintf("%032x", 1), fmt.Sprintf("%032x", 2), "ffff"}
	_, err = db.Exec("UPDATE "+store.readModelTable("trace_sources")+" SET scope_valid=FALSE WHERE org_id=? AND space_id='scope' AND trace_id=?", org, ids[0])
	require.NoError(t, err)
	batched, err := store.traceSourceBatch(ctx, org, "scope", ids)
	require.NoError(t, err)
	batchData := batched.(map[string]any)["data"].([]any)
	for i, id := range ids {
		single, err := store.traceSourceSnapshot(ctx, org, "scope", id)
		require.NoError(t, err)
		receipt := single.(map[string]any)
		receipt["trace_id"] = id
		require.Equal(t, receipt, batchData[i])
	}
	// Shared maintenance OFF immediately prevents this already-created reader
	// from trusting old summaries. Default-on restart does not clear the latch.
	require.NoError(t, ingest.SuspendProjectionMaintenance(ctx, db, prefix))
	require.NoError(t, ingest.InitializeTraceReadModelMaintenance(ctx, db, prefix, true))
	result, err = store.traceWindow(ctx, org, p, false)
	require.NoError(t, err)
	r = result.(map[string]any)
	require.Empty(t, r["rows"])
	require.Len(t, r["raw"], 3)
	require.True(t, r["meta"].(map[string]any)["raw"].(bool))
	// Generation fencing prevents an old revision-1 snapshot from publishing
	// after resume rebuilds the source table and revision numbers start at 1.
	_, err = db.Exec("UPDATE "+store.tracesTable()+" SET span_name='after-off' WHERE org_id=? AND space_id='scope' AND trace_id=?", org, id)
	require.NoError(t, err)
	require.NoError(t, ingest.ResumeProjectionMaintenance(ctx, db, prefix))
	_, err = store.backfillTraceSources(ctx, org, "scope", 100)
	require.NoError(t, err)
	snapshot, err := store.traceSourceSnapshot(ctx, org, "scope", id)
	require.NoError(t, err)
	fresh := snapshot.(map[string]any)
	require.EqualValues(t, 1, fresh["source_revision"])
	require.Greater(t, fresh["maintenance_generation"].(int64), projection.MaintenanceGeneration)
	refused, err := store.publishTraceProjection(ctx, org, projection)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("maintenance_generation_changed"), refused)
	projection.MaintenanceGeneration = fresh["maintenance_generation"].(int64)
	freshRow := map[string]any{}
	require.NoError(t, json.Unmarshal(projection.Row, &freshRow))
	freshRow["name"] = "after-off"
	projection.Row, err = json.Marshal(freshRow)
	require.NoError(t, err)
	published, err := store.publishTraceProjection(ctx, org, projection)
	require.NoError(t, err)
	require.Equal(t, true, published.(map[string]any)["published"])
	// A fully absent derived schema also supports the explicit raw emergency.
	for _, suffix := range []string{"trace_sources", "trace_projections", "trace_projection_coverage"} {
		_, err = db.Exec("DROP TABLE " + store.readModelTable(suffix))
		require.NoError(t, err)
	}
	result, err = store.traceWindow(ctx, org, p, true)
	require.NoError(t, err)
	require.Len(t, result.(map[string]any)["raw"], 3)
}

func integrationProjectionGeneration(t *testing.T, db *sql.DB, prefix string) int64 {
	t.Helper()
	var generation int64
	require.NoError(t, db.QueryRow("SELECT generation FROM `"+prefix+"_projection_maintenance` WHERE id=1").Scan(&generation))
	require.GreaterOrEqual(t, generation, int64(1))
	return generation
}
