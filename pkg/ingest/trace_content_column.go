package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const TraceContentCompleteColumn = "content_complete_v1"

// HEX preserves the old BINARY 'false' comparison, including case and trailing
// bytes, without relying on a generated column's text collation. Missing flags
// are legacy complete documents. Every row_json write updates this helper.
const TraceContentCompleteExpression = "CASE WHEN HEX(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(row_json,'$._contentComplete')),'true'))='66616C7365' THEN 0 ELSE 1 END"
const traceContentCompleteStoredExpression = "(case when (hex(coalesce(json_unquote(json_extract(`row_json`,'$._contentComplete')),'true')) = '66616C7365') then 0 else 1 end)"

func traceContentColumnDefinition(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, bool, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return false, false, fmt.Errorf("invalid table prefix")
	}
	var kind, extra string
	var expression sql.NullString
	err := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_trace_projections", TraceContentCompleteColumn).Scan(&kind, &extra, &expression)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	exact := expression.Valid && (SessionTokenGeneratedExpressionMatches(expression.String, TraceContentCompleteExpression) || SessionTokenGeneratedExpressionMatches(expression.String, traceContentCompleteStoredExpression))
	kindOK := strings.EqualFold(kind, "tinyint") || strings.EqualFold(kind, "tinyint(4)")
	return kindOK && strings.ToUpper(strings.Join(strings.Fields(extra), " ")) == "STORED GENERATED" && exact, true, nil
}

// Missing/mismatched optional columns select JSON coverage. Inspection failures
// remain errors. Query handlers never perform DDL.
func TraceContentColumnReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	ready, _, err := traceContentColumnDefinition(ctx, db, prefix)
	return ready, err
}

// This optional additive migration never disables or clears the base model.
// The shared lease serializes DDL with maintenance/statistics migrations.
func InitializeTraceContentColumn(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ready, exists, err := traceContentColumnDefinition(ctx, db, prefix)
	if err != nil || ready {
		return err
	}
	if exists {
		return fmt.Errorf("incompatible optional content-completeness column")
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
	result, err := db.ExecContext(ctx, "UPDATE "+table+" SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+300 WHERE id=1 AND enabled=TRUE AND suspended=FALSE AND rebuild_required=FALSE AND schema_version=? AND generation=? AND migration_until<UNIX_TIMESTAMP()", owner, ProjectionSchemaVersion, generation)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("content-completeness additive migration lease unavailable")
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		_, _ = db.ExecContext(releaseCtx, "UPDATE "+table+" SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=? AND generation=?", owner, generation)
	}()
	return withProjectionMigrationConnection(ctx, db, func(conn *sql.Conn) error {
		ready, exists, err := traceContentColumnDefinition(ctx, conn, prefix)
		if err != nil || ready {
			return err
		}
		if exists {
			return fmt.Errorf("incompatible optional content-completeness column")
		}
		if _, err := conn.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" ADD COLUMN "+TraceContentCompleteColumn+" TINYINT GENERATED ALWAYS AS ("+TraceContentCompleteExpression+") STORED"); err != nil {
			return err
		}
		ready, _, err = traceContentColumnDefinition(ctx, conn, prefix)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("content-completeness column read-back failed")
		}
		return nil
	})
}
