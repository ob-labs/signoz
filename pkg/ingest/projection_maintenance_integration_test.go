package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

// The test owns a unique prefix. It can run against either engine without
// touching application tables or the retained performance datasets.
func TestOceanBaseProjectionMaintenanceLifecycle(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for isolated real SQL validation")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	prefix := fmt.Sprintf("maint_%x", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage", "projection_maintenance", "traces", "logs", "metric_samples"} {
		suffix := suffix
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if _, err := db.ExecContext(cleanupCtx, "DROP TABLE IF EXISTS "+tableName(prefix, suffix)); err != nil {
				t.Errorf("cleanup %s: %v", suffix, err)
			}
		})
	}
	if err := ensureSchema(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	if err := EnsureTraceReadModelSchema(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	// Reproduce an earlier development schema; the initializer must migrate it
	// automatically and tolerate retry after a partially applied migration.
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" DROP COLUMN "+SessionTokenGeneratedValueColumn+",DROP COLUMN "+SessionTokenGeneratedBillableColumn); err != nil {
		t.Fatal(err)
	}
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		t.Fatal(err)
	}
	var generated int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND EXTRA='STORED GENERATED'", prefix+"_trace_projections").Scan(&generated); err != nil || generated != 2 {
		t.Fatalf("generated=%d err=%v", generated, err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO "+tableName(prefix, "trace_projection_coverage")+" (org_id,space_id,after_trace_id,complete) VALUES ('org','space','abc',TRUE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO "+tableName(prefix, "trace_sources")+" (org_id,space_id,trace_id,source_revision,min_timestamp,max_timestamp,dirty) VALUES ('org','space','abc',1,1,1,FALSE)"); err != nil {
		t.Fatal(err)
	}
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		t.Fatal(err)
	}
	var complete bool
	var cursor string
	if err := db.QueryRowContext(ctx, "SELECT complete,after_trace_id FROM "+tableName(prefix, "trace_projection_coverage")).Scan(&complete, &cursor); err != nil || !complete || cursor != "abc" {
		t.Fatalf("restart complete=%v cursor=%q err=%v", complete, cursor, err)
	}
	if err := SuspendProjectionMaintenance(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, generation, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || ready || generation < 2 {
		t.Fatalf("ordinary restart undid shared OFF latch: ready=%v generation=%d err=%v", ready, generation, err)
	}
	// An already-running default-on writer must obey another replica's OFF latch
	// and keep raw writes available without inventing a tracked revision.
	ep := &endpoint{db: db, cfg: Config{TablePrefix: prefix}, tracesTable: spansTable(prefix)}
	columns := " (org_id,space_id,timestamp,trace_id,span_id,span_name,span_kind,start_time_unix_nano,end_time_unix_nano,duration_nano,scope_attributes,resource_attributes,attributes,events,links,payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	values := []any{"org", "space", int64(1), "def", "0000000000000001", "sample", "INTERNAL", int64(1), int64(2), int64(1), "{}", "{}", "{}", "[]", "[]", "{}"}
	if err := ep.insertRows(ctx, ep.tracesTable, columns, "", [][]any{values}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName(prefix, "trace_sources")+" WHERE trace_id='def'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("OFF writer created derived source: count=%d err=%v", count, err)
	}
	if err := ResumeProjectionMaintenance(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"trace_sources", "trace_projection_coverage"} {
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName(prefix, suffix)).Scan(&count); err != nil || count != 0 {
			t.Fatalf("resume left orphan %s: count=%d err=%v", suffix, count, err)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+spansTable(prefix)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("resume changed raw spans: count=%d err=%v", count, err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, newGeneration, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || !ready || newGeneration <= generation {
		t.Fatalf("resume failed fencing: ready=%v generation=%d err=%v", ready, newGeneration, err)
	}
	// A full emergency rollback may remove all derived tables. Default-on
	// startup must repair them while keeping the operator's OFF latch intact.
	if err := SuspendProjectionMaintenance(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE "+tableName(prefix, suffix)); err != nil {
			t.Fatal(err)
		}
	}
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (?,?,?)", prefix+"_trace_projections", prefix+"_trace_sources", prefix+"_trace_projection_coverage").Scan(&count); err != nil || count != 3 {
		t.Fatalf("default-on restart did not repair missing derived schema: tables=%d err=%v", count, err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, repairGeneration, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || ready || repairGeneration <= newGeneration {
		t.Fatalf("schema repair undid OFF latch or reused generation: ready=%v generation=%d err=%v", ready, repairGeneration, err)
	}
	if err := ResumeProjectionMaintenance(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+spansTable(prefix)).Scan(&count); err != nil || count != 1 {
		t.Fatalf("schema repair changed raw spans: count=%d err=%v", count, err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, newGeneration, err = ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || !ready || newGeneration <= repairGeneration {
		t.Fatalf("repaired schema did not resume: ready=%v generation=%d err=%v", ready, newGeneration, err)
	}
	// A partial rollback can leave the current version record but omit a Token
	// column. Repair that drift as well, advancing the generation before reset.
	if _, err := db.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" DROP COLUMN "+SessionTokenGeneratedValueColumn); err != nil {
		t.Fatal(err)
	}
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND EXTRA='STORED GENERATED'", prefix+"_trace_projections").Scan(&generated); err != nil || generated != 2 {
		t.Fatalf("partial Token rollback was not repaired: generated=%d err=%v", generated, err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, columnRepairGeneration, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || !ready || columnRepairGeneration <= newGeneration {
		t.Fatalf("Token column repair reused generation: ready=%v generation=%d err=%v", ready, columnRepairGeneration, err)
	}
	newGeneration = columnRepairGeneration
	// An OFF writer is also an invalidation path. Check the engine's actual
	// left-to-right UPDATE assignment semantics rather than only SQL text.
	ep.cfg.ProjectionMaintenanceDisabled = true
	values[3] = "fed"
	if err := ep.insertRows(ctx, ep.tracesTable, columns, "", [][]any{values}); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ready, offWriterGeneration, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || ready || offWriterGeneration != newGeneration+1 {
		t.Fatalf("OFF writer failed generation invalidation: ready=%v prior=%d after=%d err=%v", ready, newGeneration, offWriterGeneration, err)
	}
	if err := SuspendProjectionMaintenance(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, repeatedOffGeneration, err := ReadProjectionMaintenance(ctx, tx, prefix)
	_ = tx.Rollback()
	if err != nil || repeatedOffGeneration != offWriterGeneration {
		t.Fatalf("already-OFF suspension should preserve generation: prior=%d after=%d err=%v", offWriterGeneration, repeatedOffGeneration, err)
	}
}
