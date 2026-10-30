package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

const TracePageDimensionsColumn = "page_dimensions_v1"
const TracePageSourceBytesColumn = "page_source_bytes_v1"

var TracePageSourceBytesExpression = func() string {
	parts := []string{}
	for _, column := range []string{"trace_id", "span_id", "parent_span_id", "span_name", "span_kind", "status_message", "service_name", "user_id", "space_id", "agent_product", "session_id", "resource_attributes", "attributes", "events", "links", "payload"} {
		parts = append(parts, "OCTET_LENGTH(COALESCE("+column+",''))")
	}
	result := parts[0]
	for _, part := range append(parts[1:], "2048") {
		result = "(" + result + "+" + part + ")"
	}
	return result
}()

// Store only filter/facet/scope metadata. Large IO payloads are parsed on write,
// never for every span in a list query. Generated storage also covers legacy
// writers, updates and backfilled rows without a second consistency protocol.
var TracePageDimensionsExpression = func() string {
	key := func(column, name string) string { return `JSON_EXTRACT(` + column + `,'$."` + name + `"')` }
	has := func(column, name string) string { return `JSON_CONTAINS_PATH(` + column + `,'one','$."` + name + `"')` }
	attr := func(name string) string {
		return "(CASE WHEN " + has("attributes", name) + " THEN " + key("attributes", name) + " ELSE " + key("JSON_EXTRACT(payload,'$.attributes')", name) + " END)"
	}
	hasAttr := func(name string) string {
		return "(CASE WHEN " + has("attributes", name) + " THEN 1 ELSE (CASE WHEN " + has("JSON_EXTRACT(payload,'$.attributes')", name) + " THEN 1 ELSE 0 END) END)"
	}
	parts := []string{}
	for _, pair := range [][2]string{{"space", "aivision.space.id"}, {"agent", "aivision.agent.id"}, {"env", "ant.agent.env"}, {"scene", "ant.agent.scene"}, {"resource_username", "ant.username"}} {
		parts = append(parts, "'"+pair[0]+"'", key("resource_attributes", pair[1]), "'has_"+pair[0]+"'", "(CASE WHEN "+has("resource_attributes", pair[1])+" THEN 1 ELSE 0 END)")
	}
	for _, pair := range [][2]string{{"attribute_username", "ant.username"}, {"tags", "langfuse.trace.tags"}} {
		parts = append(parts, "'"+pair[0]+"'", attr(pair[1]), "'has_"+pair[0]+"'", hasAttr(pair[1]))
	}
	return "JSON_OBJECT(" + strings.Join(parts, ",") + ")"
}()

func TracePageDimensionsReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return false, fmt.Errorf("invalid table prefix")
	}
	for _, column := range [][3]string{{TracePageDimensionsColumn, "longtext", TracePageDimensionsExpression}, {TracePageSourceBytesColumn, "bigint", TracePageSourceBytesExpression}} {
		var kind, extra string
		var expression sql.NullString
		err := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_traces", column[0]).Scan(&kind, &extra, &expression)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		kindOK := strings.EqualFold(kind, column[1]) || (column[1] == "bigint" && strings.EqualFold(kind, "bigint(20)"))
		if !kindOK || !strings.EqualFold(extra, "STORED GENERATED") || !expression.Valid || !(SessionTokenGeneratedExpressionMatches(expression.String, column[2]) || SessionTokenGeneratedExpressionMatches(expression.String, strings.ReplaceAll(column[2], `"`, `\"`))) {
			return false, nil
		}
	}
	return true, nil
}

// Optional additive startup migration. Queries never perform DDL. A timeout or
// incompatible existing column keeps ingestion running and pagination fails
// explicitly until an operator finishes the migration; no unbounded fallback.
func InitializeTracePageDimensions(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	ready, err := TracePageDimensionsReady(ctx, db, prefix)
	if err != nil || ready {
		return err
	}
	return withProjectionMigrationConnection(ctx, db, func(conn *sql.Conn) error {
		var ddlErr error
		for _, column := range [][3]string{{TracePageDimensionsColumn, "LONGTEXT", TracePageDimensionsExpression}, {TracePageSourceBytesColumn, "BIGINT", TracePageSourceBytesExpression}} {
			var count int
			if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_traces", column[0]).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				if _, err := conn.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "traces")+" ADD COLUMN "+column[0]+" "+column[1]+" GENERATED ALWAYS AS ("+column[2]+") STORED"); err != nil {
					ddlErr = err
				}
			}
		}
		// Another initializer may have won the additive DDL race. Verify exactly.
		ready, checkErr := TracePageDimensionsReady(ctx, conn, prefix)
		if checkErr != nil {
			return checkErr
		}
		if ready {
			return nil
		}
		if ddlErr != nil {
			return ddlErr
		}
		return fmt.Errorf("trace page metadata column read-back failed")
	})
}
