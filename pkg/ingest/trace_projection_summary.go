package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Stored columns may rebuild historical multi-GB projection tables. This is
// maintenance-only work: API/query deadlines and source/wire budgets do not
// inherit it. Ownership outlives the DDL deadline by a cleanup margin.
const TraceProjectionSummaryMigrationTimeout = 15 * time.Minute
const traceProjectionSummaryLeaseDuration = TraceProjectionSummaryMigrationTimeout + 5*time.Minute

const TraceProjectionMetadataColumn = "metadata_json_v1"
const TraceProjectionMetricsColumn = "metrics_json_v1"
const TraceProjectionMetricsExpression = "JSON_EXTRACT(row_json,'$._metrics')"
const TraceProjectionRowKeysColumn = "row_keys_json_v1"
const TraceProjectionRowKeysExpression = "JSON_KEYS(row_json)"
const TraceProjectionContentColumn = "content_json_v1"
const TraceProjectionOperationPreviewsColumn = "operation_previews_json_v1"
const TraceProjectionOperationPreviewsExpression = "JSON_EXTRACT(row_json,'$._operation_previews')"
const TraceProjectionMetricsVersionColumn = "metrics_version_v1"
const TraceProjectionMetricsVersionExpression = "JSON_UNQUOTE(JSON_EXTRACT(row_json,'$._metrics.version'))"
const TraceProjectionContentExpression = "JSON_REMOVE(row_json,'$._operation_previews','$._statistics','$._metrics')"

// These are the canonical list projections. Persist the exact former query
// expressions so JSON parsing happens on publication/backfill, never before a
// list's byte preflight. Generated columns also cover older publisher binaries.
var TraceProjectionMetadataExpression = func() string {
	fields := []string{
		"trace_id", "name", "userId", "session_id", "environment", "environmentSource",
		"start_time_unix_nano", "end_time_unix_nano", "duration_ms", "status_code", "span_count",
		"usage", "coverage", "costCoverage", "reportedUsage", "rootObservationId", "rootSelectionReason", "missingParentIds",
		"input_tokens", "output_tokens", "total_tokens",
		"attributes.gen_ai.span.kind", "attributes.gen_ai.request.model",
		"attributes.gen_ai.usage.input_tokens", "attributes.gen_ai.usage.output_tokens", "attributes.gen_ai.usage.total_tokens",
		"attributes.langfuse.trace.tags", "resource.attributes.ant.agent.env", "resource.attributes.ant.agent.scene",
		"resource.attributes.aivision.agent.id", "resource.attributes.aivision.space.id", "resource.attributes.ant.agent.product",
		"attributes.ant.username", "attributes.gen_ai.tool.call.name", "attributes.gen_ai.operation.name", "attributes.gen_ai.provider.name",
		"_traceTags", "_sceneValues", "_contentComplete",
	}
	values := make([]string, 0, (len(fields)+4)*2)
	for _, field := range fields {
		// Quoted JSON path members address literal dotted keys, rather than
		// interpreting native aliases as nested objects. All keys are constants.
		path, _ := json.Marshal(field)
		values = append(values, "'"+field+"'", "JSON_EXTRACT(row_json,'$."+string(path)+"')")
	}
	// Legacy validators require these canonical keys. Explicit null preserves
	// their shape without selecting a full body; ioPreview is a separate column.
	for _, field := range []string{"attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "attributes.gen_ai.tool.call.arguments", "attributes.gen_ai.tool.call.result"} {
		values = append(values, "'"+field+"'", "NULL")
	}
	return "JSON_OBJECT(" + strings.Join(values, ",") + ")"
}()

func traceProjectionSummaryColumns() [][3]string {
	return [][3]string{{TraceProjectionMetadataColumn, "LONGTEXT", TraceProjectionMetadataExpression}, {TraceProjectionContentColumn, "LONGTEXT", TraceProjectionContentExpression}, {TraceProjectionMetricsVersionColumn, "LONGTEXT", TraceProjectionMetricsVersionExpression}, {TraceProjectionOperationPreviewsColumn, "LONGTEXT", TraceProjectionOperationPreviewsExpression}, {TraceProjectionMetricsColumn, "LONGTEXT", TraceProjectionMetricsExpression}, {TraceProjectionRowKeysColumn, "LONGTEXT", TraceProjectionRowKeysExpression}}
}

func traceProjectionSummaryDefinition(ctx context.Context, db statisticsSchemaReader, prefix string, column [3]string) (bool, bool, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return false, false, fmt.Errorf("invalid table prefix")
	}
	var kind, extra string
	var expression sql.NullString
	err := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_trace_projections", column[0]).Scan(&kind, &extra, &expression)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	exact := expression.Valid && (SessionTokenGeneratedExpressionMatches(expression.String, column[2]) || SessionTokenGeneratedExpressionMatches(expression.String, strings.ReplaceAll(column[2], `"`, `\"`)))
	return strings.EqualFold(kind, column[1]) && strings.ToUpper(strings.Join(strings.Fields(extra), " ")) == "STORED GENERATED" && exact, true, nil
}

// Query handlers inspect capability inside their snapshot and never run DDL.
// Ordinary/virtual/conflicting columns cannot establish a persisted-size proof.
func TraceProjectionSummaryColumnsReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	for _, column := range traceProjectionSummaryColumns()[:2] {
		ready, _, err := traceProjectionSummaryDefinition(ctx, db, prefix, column)
		if err != nil || !ready {
			return false, err
		}
	}
	return true, nil
}

// Supplement enrollment only needs the exact persisted version. It must not
// parse every full canonical document while looking for the next dirty page.
func TraceProjectionMetricsVersionReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	ready, _, err := traceProjectionSummaryDefinition(ctx, db, prefix, [3]string{TraceProjectionMetricsVersionColumn, "LONGTEXT", TraceProjectionMetricsVersionExpression})
	return ready, err
}

func TraceProjectionOperationPreviewsReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	ready, _, err := traceProjectionSummaryDefinition(ctx, db, prefix, [3]string{TraceProjectionOperationPreviewsColumn, "LONGTEXT", TraceProjectionOperationPreviewsExpression})
	return ready, err
}

// Metrics recovery needs both complete observations and a presence proof: the
// existing metadata object intentionally supplies null for absent fields.
func TraceProjectionMetricsWindowColumnsReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	for _, column := range [][3]string{
		{TraceProjectionMetadataColumn, "LONGTEXT", TraceProjectionMetadataExpression},
		{TraceProjectionMetricsColumn, "LONGTEXT", TraceProjectionMetricsExpression},
		{TraceProjectionRowKeysColumn, "LONGTEXT", TraceProjectionRowKeysExpression},
		{TraceProjectionOperationPreviewsColumn, "LONGTEXT", TraceProjectionOperationPreviewsExpression},
	} {
		ready, _, err := traceProjectionSummaryDefinition(ctx, db, prefix, column)
		if err != nil || !ready {
			return false, err
		}
	}
	return true, nil
}

// The shared lease coordinates additive DDL with maintenance/statistics work
// while unexpired. Expiry does not prove a remote ALTER has stopped.
// ALTER backfills existing projections without invalidating their revisions,
// coverage, or the durable operator OFF state. Conflicting columns are retained.
func InitializeTraceProjectionSummaries(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, TraceProjectionSummaryMigrationTimeout)
	defer cancel()
	missing := false
	for _, column := range traceProjectionSummaryColumns() {
		ready, exists, err := traceProjectionSummaryDefinition(ctx, db, prefix, column)
		if err != nil {
			return err
		}
		if exists && !ready {
			return fmt.Errorf("incompatible projection summary column %s", column[0])
		}
		missing = missing || !ready
	}
	if !missing {
		return nil
	}
	table, err := projectionMaintenanceTable(prefix)
	if err != nil {
		return err
	}
	var generation int64
	err = db.QueryRowContext(ctx, "SELECT generation FROM "+table+" WHERE id=1 AND enabled=TRUE AND suspended=FALSE AND rebuild_required=FALSE AND schema_version=?", ProjectionSchemaVersion).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	owner, err := newProjectionCleanupOwner()
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, "UPDATE "+table+fmt.Sprintf(" SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+%d WHERE", int64(traceProjectionSummaryLeaseDuration/time.Second))+" id=1 AND enabled=TRUE AND suspended=FALSE AND rebuild_required=FALSE AND schema_version=? AND generation=? AND migration_until<UNIX_TIMESTAMP()", owner, ProjectionSchemaVersion, generation)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("projection summary additive migration lease unavailable")
	}
	ddlStarted, ddlCompleted := false, false
	defer func() {
		// Cancellation or a connection error does not prove the server has
		// stopped an ALTER. Retain the lease instead of releasing it early
		// unless the server acknowledged completion. Its TTL is coordination,
		// not a server-side DDL deadline or proof of termination.
		if ddlStarted && !ddlCompleted {
			return
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		_, _ = db.ExecContext(releaseCtx, "UPDATE "+table+" SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=? AND generation=?", owner, generation)
	}()
	return withProjectionMigrationTimeout(ctx, db, TraceProjectionSummaryMigrationTimeout, func(conn *sql.Conn) error {
		additions := []string{}
		for _, column := range traceProjectionSummaryColumns() {
			ready, exists, err := traceProjectionSummaryDefinition(ctx, conn, prefix, column)
			if err != nil {
				return err
			}
			if ready {
				continue
			}
			if exists {
				return fmt.Errorf("incompatible projection summary column %s", column[0])
			}
			additions = append(additions, "ADD COLUMN "+column[0]+" "+column[1]+" GENERATED ALWAYS AS ("+column[2]+") STORED")
		}
		if len(additions) > 0 {
			// These expressions depend only on row_json. Add all missing columns
			// together so an upgrade backfills the existing table once, after
			// every existing definition has been rechecked under the lease.
			ddlStarted = true
			if _, err := conn.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" "+strings.Join(additions, ",")); err != nil {
				return err
			}
			ddlCompleted = true
		}
		for _, column := range traceProjectionSummaryColumns() {
			ready, _, err := traceProjectionSummaryDefinition(ctx, conn, prefix, column)
			if err != nil {
				return err
			}
			if !ready {
				return fmt.Errorf("projection summary column read-back failed: %s", column[0])
			}
		}
		return nil
	})
}
