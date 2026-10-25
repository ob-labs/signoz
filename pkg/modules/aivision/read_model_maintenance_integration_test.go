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

// This opt-in test creates, writes and drops only its own uniquely named tables.
// It deliberately shares one data plane between two maintenance callers; no
// original table, operator control, database statistics or index is changed.
func TestOceanBaseProjectionMaintenanceConcurrency(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for isolated concurrent maintenance SQL validation")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "maint_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	org := valuer.GenerateUUID().StringValue()
	require.NoError(t, ingest.AddToRouter(mux.NewRouter(), ingest.Config{DSN: dsn, TablePrefix: prefix, OrgID: org, InternalKey: "isolated-concurrent-maintenance"}, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		for _, suffix := range []string{"traces", "logs", "metric_samples", "trace_sources", "trace_projections", "trace_projection_coverage", "projection_maintenance"} {
			_, err := db.Exec("DROP TABLE IF EXISTS `" + prefix + "_" + suffix + "`")
			require.NoError(t, err)
		}
	})
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: prefix}}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	fixture := tracePageIntegrationFixture{db: db, table: store.tracesTable(), org: org, start: start}
	const count = 63 // More than two maximum-size maintenance transactions.
	spans := make([]tracePageIntegrationSpan, 0, 2*count)
	for i := 1; i <= count; i++ {
		root, child := fixture.pair("scope", i)
		root.start += int64(i * 100)
		child.start += int64(i * 100)
		spans = append(spans, root, child)
	}
	// Direct fixture writes precede every maintenance caller. Production raw
	// ingestion takes the maintenance fence before modifying the same sources.
	fixture.insert(t, spans)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	t.Run("two_callers_share_the_committed_cursor", func(t *testing.T) {
		type outcome struct {
			enrolled int64
			err      error
		}
		results := make(chan outcome, 2)
		begin := make(chan struct{})
		for worker := 0; worker < 2; worker++ {
			go func() {
				<-begin
				var enrolled int64
				for round := 0; round < 10; round++ {
					value, err := store.backfillTraceSources(ctx, org, "scope", 500)
					if err != nil {
						results <- outcome{err: err}
						return
					}
					result, ok := value.(map[string]any)
					if !ok || result["supported"] != true {
						results <- outcome{err: fmt.Errorf("maintenance did not return supported progress: %v", value)}
						return
					}
					progress, err := asInt64(result["enrolled"])
					if err != nil {
						results <- outcome{err: err}
						return
					}
					enrolled += progress
					if result["complete"] == true {
						results <- outcome{enrolled: enrolled}
						return
					}
				}
				results <- outcome{err: fmt.Errorf("maintenance did not finish bounded fixture")}
			}()
		}
		close(begin)
		var total int64
		outcomes := make([]outcome, 0, 2)
		for worker := 0; worker < 2; worker++ {
			result := <-results
			if result.err != nil {
				cancel()
			}
			outcomes = append(outcomes, result)
		}
		for _, result := range outcomes {
			require.NoError(t, result.err)
			total += result.enrolled
		}
		require.EqualValues(t, count, total, "re-reading each committed cursor must not enroll sources twice")
		rows, err := db.QueryContext(ctx, "SELECT trace_id,source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid FROM "+store.readModelTable("trace_sources")+" WHERE org_id=? AND space_id=? ORDER BY trace_id", org, "scope")
		require.NoError(t, err)
		defer rows.Close()
		seen := 0
		for rows.Next() {
			seen++
			var id, user, product string
			var revision, min, max int64
			var valid bool
			require.NoError(t, rows.Scan(&id, &revision, &min, &max, &user, &product, &valid))
			require.Equal(t, fmt.Sprintf("%032x", seen), id)
			require.EqualValues(t, 1, revision)
			require.Equal(t, start.UnixNano()+int64(seen*100), min)
			require.Equal(t, min+1, max)
			require.Equal(t, "subject", user)
			require.Equal(t, "Codex", product)
			require.True(t, valid)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, count, seen)
		var cursor string
		var complete bool
		require.NoError(t, db.QueryRowContext(ctx, "SELECT after_trace_id,complete FROM "+store.readModelTable("trace_projection_coverage")+" WHERE org_id=? AND space_id=?", org, "scope").Scan(&cursor, &complete))
		require.True(t, complete)
		require.Equal(t, fmt.Sprintf("%032x", count), cursor)
	})

	t.Run("completed_poll_does_not_wait_for_the_maintenance_write_lock", func(t *testing.T) {
		holder, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		require.NoError(t, err)
		defer holder.Rollback()
		ready, _, err := ingest.LockProjectionMaintenance(ctx, holder, prefix)
		require.NoError(t, err)
		require.True(t, ready)
		pollContext, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		result, err := store.backfillTraceSources(pollContext, org, "scope", 500)
		require.NoError(t, err, "completed polling must read a consistent snapshot without competing for the global write fence")
		require.Equal(t, true, result.(map[string]any)["complete"])
		require.EqualValues(t, 0, result.(map[string]any)["enrolled"])
		require.NoError(t, holder.Rollback())
	})

	trace := fmt.Sprintf("%032x", 1)
	projection := validProjection()
	projection.MaintenanceGeneration = integrationProjectionGeneration(t, db, prefix)
	projection.SpaceID, projection.TraceID = "scope", trace
	projection.UserID, projection.AgentProduct = "subject", "Codex"
	projection.Period = start.UnixMilli()
	projection.Row, err = json.Marshal(map[string]any{"trace_id": trace, "start_time_unix_nano": fmt.Sprint(start.UnixNano() + 100)})
	require.NoError(t, err)
	for _, change := range []string{"source_revision", "maintenance_generation"} {
		t.Run("waiting_publication_rechecks_"+change, func(t *testing.T) {
			holder, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			require.NoError(t, err)
			defer holder.Rollback()
			ready, _, err := ingest.LockProjectionMaintenance(ctx, holder, prefix)
			require.NoError(t, err)
			require.True(t, ready)
			type publication struct {
				value any
				err   error
			}
			done := make(chan publication, 1)
			go func() {
				value, err := store.publishTraceProjection(ctx, org, projection)
				done <- publication{value, err}
			}()
			// Observe the actual blocked production SQL before changing the row.
			// A fixed sleep could pass without exercising the stale RR snapshot.
			require.NoError(t, waitForMaintenanceFenceQuery(ctx, db, prefix))
			if change == "source_revision" {
				_, err = holder.ExecContext(ctx, "UPDATE "+store.readModelTable("trace_sources")+" SET source_revision=source_revision+1,dirty=TRUE WHERE org_id=? AND space_id=? AND trace_id=?", org, "scope", trace)
			} else {
				_, err = holder.ExecContext(ctx, "UPDATE "+store.readModelTable("projection_maintenance")+" SET generation=generation+1 WHERE id=1")
			}
			require.NoError(t, err)
			require.NoError(t, holder.Commit())
			result := <-done
			require.NoError(t, result.err, "a writer waiting for the fence must not fail with a stale RR serialization error")
			reason := "source_revision_changed"
			if change == "maintenance_generation" {
				reason = "maintenance_generation_changed"
			}
			require.Equal(t, readModelUnsupported(reason), result.value)
			projection.SourceRevision = 2
			projection.MaintenanceGeneration = integrationProjectionGeneration(t, db, prefix)
			fresh, err := store.publishTraceProjection(ctx, org, projection)
			require.NoError(t, err)
			require.Equal(t, true, fresh.(map[string]any)["published"])
		})
	}
	var rawCount int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+store.tracesTable()).Scan(&rawCount))
	require.Equal(t, 2*count, rawCount)
}

func waitForMaintenanceFenceQuery(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		rows, err := db.QueryContext(ctx, "SHOW FULL PROCESSLIST")
		if err != nil {
			return err
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return err
		}
		found := false
		for rows.Next() {
			values := make([]any, len(columns))
			refs := make([]any, len(columns))
			for i := range values {
				refs[i] = &values[i]
			}
			if err = rows.Scan(refs...); err != nil {
				break
			}
			for i, column := range columns {
				if strings.EqualFold(column, "Info") {
					query := fmt.Sprint(values[i])
					if text, ok := values[i].([]byte); ok {
						query = string(text)
					}
					found = found || strings.Contains(query, prefix+"_projection_maintenance") && strings.Contains(query, "FOR UPDATE")
				}
			}
		}
		if rowsErr := rows.Err(); err == nil {
			err = rowsErr
		}
		rows.Close()
		if err != nil {
			return err
		}
		if found {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("publication did not reach the maintenance fence: %w", ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
