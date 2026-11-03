package ingest

// Optional AP storage has its own additive migration. Failure never revokes the
// existing Trace/Session read model or changes its generation/schema version.
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const StatisticsReceiptColumn = "statistics_receipt_v1"
const StatisticsReceiptExpression = "JSON_UNQUOTE(JSON_EXTRACT(row_json,'$._statistics.receipt'))"

var statisticsVersionPattern = regexp.MustCompile(`(?i)OceanBase(?:_CE)?-v([0-9]+)\.([0-9]+)`)
var statisticsSeekdbVersionPattern = regexp.MustCompile(`(?i)^(?:[0-9]+\.[0-9]+\.[0-9]+-)?(?:OceanBase )?seekdb-v([0-9]+)\.([0-9]+)\.[0-9]+(?:\.[0-9]+)?$`)

func TraceStatisticsVersionSupported(version string) bool {
	if strings.Contains(strings.ToLower(version), "seekdb") {
		parts := statisticsSeekdbVersionPattern.FindStringSubmatch(version)
		if len(parts) != 3 {
			return false
		}
		major, e := strconv.Atoi(parts[1])
		if e != nil {
			return false
		}
		minor, e := strconv.Atoi(parts[2])
		return e == nil && major == 1 && minor >= 4
	}
	parts := statisticsVersionPattern.FindStringSubmatch(version)
	if len(parts) != 3 {
		return false
	}
	major, e := strconv.Atoi(parts[1])
	if e != nil {
		return false
	}
	minor, e := strconv.Atoi(parts[2])
	if e != nil {
		return false
	}
	return major > 4 || major == 4 && minor >= 4
}

type statisticsSchemaReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func statisticsReceiptMatches(kind, extra, expression string) bool {
	normalize := func(s string) string {
		var out strings.Builder
		quoted := false
		for _, c := range s {
			if c == '\'' {
				quoted = !quoted
				out.WriteRune(c)
			} else if quoted {
				out.WriteRune(c)
			} else if c != '`' && c != ' ' && c != '\n' && c != '\t' && c != '\r' {
				out.WriteString(strings.ToLower(string(c)))
			}
		}
		return out.String()
	}
	actual, want := normalize(expression), normalize(StatisticsReceiptExpression)
	return strings.EqualFold(kind, "char(64)") && strings.ToUpper(strings.Join(strings.Fields(extra), " ")) == "STORED GENERATED" && (actual == want || actual == "("+want+")")
}

func TraceStatisticsSchemaReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return false, fmt.Errorf("invalid table prefix")
	}
	var count int
	if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (?,?)", prefix+"_trace_statistics", prefix+"_call_statistics").Scan(&count); e != nil {
		return false, e
	}
	if count != 2 {
		return false, nil
	}
	var kind, extra string
	var expression sql.NullString
	e := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_trace_projections", StatisticsReceiptColumn).Scan(&kind, &extra, &expression)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if !expression.Valid || !statisticsReceiptMatches(kind, extra, expression.String) {
		return false, nil
	}
	return statisticsTablesReady(ctx, db, prefix)
}

// An existing name is not proof of a usable typed schema. Check types,
// precision, nullability and the exact primary key before trusting its values.
func statisticsTablesReady(ctx context.Context, db statisticsSchemaReader, prefix string) (bool, error) {
	type column struct {
		name, kind               string
		nullable                 bool
		precision, scale, length int
	}
	shared := []column{{"org_id", "varchar", false, 0, 0, 255}, {"space_id", "varchar", false, 0, 0, 255}, {"trace_id", "varchar", false, 0, 0, 32}}
	trace := append(append([]column{}, shared...), column{"source_revision", "bigint", false, 0, 0, 0}, column{"projection_context", "char", false, 0, 0, 64}, column{"receipt", "char", false, 0, 0, 64}, column{"eligible", "tinyint", false, 0, 0, 0}, column{"span_count", "bigint", false, 0, 0, 0}, column{"generation_count", "bigint", false, 0, 0, 0}, column{"tool_count", "bigint", false, 0, 0, 0}, column{"latency_ms", "double", true, 0, 0, 0}, column{"error_count", "tinyint", false, 0, 0, 0})
	calls := append(append([]column{}, shared...), column{"observation_id", "varchar", false, 0, 0, 255}, column{"kind", "varchar", false, 0, 0, 16}, column{"start_ns", "bigint", false, 0, 0, 0}, column{"model", "varchar", true, 0, 0, 255}, column{"provider", "varchar", true, 0, 0, 255}, column{"ambiguous", "tinyint", false, 0, 0, 0})
	for _, field := range []string{"input", "output", "total", "cache_read_input", "cache_write_input", "reasoning"} {
		calls = append(calls, column{field + "_tokens", "decimal", true, 65, 0, 0})
	}
	for _, field := range []string{"total", "input", "output"} {
		calls = append(calls, column{"cost_" + field, "decimal", true, 65, 18, 0})
	}
	calls = append(calls, column{"cost_currency", "varchar", true, 0, 0, 16})
	predicates := []string{}
	args := []any{}
	for _, table := range []struct {
		suffix  string
		columns []column
	}{{"trace_statistics", trace}, {"call_statistics", calls}} {
		for _, c := range table.columns {
			predicate := "(TABLE_NAME=? AND COLUMN_NAME=? AND DATA_TYPE=? AND IS_NULLABLE=?"
			nullable := "NO"
			if c.nullable {
				nullable = "YES"
			}
			args = append(args, prefix+"_"+table.suffix, c.name, c.kind, nullable)
			if c.length > 0 {
				predicate += " AND CHARACTER_MAXIMUM_LENGTH=?"
				args = append(args, c.length)
			}
			if c.precision > 0 {
				predicate += " AND NUMERIC_PRECISION=? AND NUMERIC_SCALE=?"
				args = append(args, c.precision, c.scale)
			}
			predicates = append(predicates, predicate+")")
		}
	}
	var count int
	if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND ("+strings.Join(predicates, " OR ")+")", args...).Scan(&count); e != nil {
		return false, e
	}
	if count != len(trace)+len(calls) {
		return false, nil
	}
	var primary, matching int
	query := `SELECT COUNT(*),COALESCE(SUM(CASE WHEN (SEQ_IN_INDEX=1 AND COLUMN_NAME='org_id') OR (SEQ_IN_INDEX=2 AND COLUMN_NAME='space_id') OR (SEQ_IN_INDEX=3 AND COLUMN_NAME='trace_id') OR (TABLE_NAME=? AND SEQ_IN_INDEX=4 AND COLUMN_NAME='observation_id') THEN 1 ELSE 0 END),0) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (?,?) AND INDEX_NAME='PRIMARY'`
	if e := db.QueryRowContext(ctx, query, prefix+"_call_statistics", prefix+"_trace_statistics", prefix+"_call_statistics").Scan(&primary, &matching); e != nil {
		return false, e
	}
	return primary == 7 && matching == 7, nil
}

// TraceStatisticsDDL is also used by isolated layout benchmarks. Ordinary
// startup uses row storage for the narrow facts; measured 10k layouts had
// overlapping latency while row storage used less space. Existing hybrid or
// column layouts remain compatible and are never converted automatically.
func TraceStatisticsDDL(prefix string) ([]string, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf("invalid table prefix")
	}
	columns := []string{}
	for _, field := range []string{"input", "output", "total", "cache_read_input", "cache_write_input", "reasoning"} {
		columns = append(columns, field+"_tokens DECIMAL(65,0) NULL")
	}
	return []string{
		"CREATE TABLE IF NOT EXISTS " + tableName(prefix, "trace_statistics") + ` (
 org_id VARCHAR(255) NOT NULL,space_id VARCHAR(255) NOT NULL,trace_id VARCHAR(32) NOT NULL,
 source_revision BIGINT UNSIGNED NOT NULL,projection_context CHAR(64) NOT NULL,receipt CHAR(64) NOT NULL,eligible BOOLEAN NOT NULL,
 span_count BIGINT NOT NULL,generation_count BIGINT NOT NULL,tool_count BIGINT NOT NULL,
 latency_ms DOUBLE NULL,error_count TINYINT NOT NULL,
 PRIMARY KEY(org_id,space_id,trace_id)
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin WITH COLUMN GROUP(all columns)`,
		"CREATE TABLE IF NOT EXISTS " + tableName(prefix, "call_statistics") + ` (
 org_id VARCHAR(255) NOT NULL,space_id VARCHAR(255) NOT NULL,trace_id VARCHAR(32) NOT NULL,
 observation_id VARCHAR(255) NOT NULL,kind VARCHAR(16) NOT NULL,start_ns BIGINT UNSIGNED NOT NULL,
 model VARCHAR(255) NULL,provider VARCHAR(255) NULL,ambiguous BOOLEAN NOT NULL,
 ` + strings.Join(columns, ",") + `,
 cost_total DECIMAL(65,18) NULL,cost_input DECIMAL(65,18) NULL,cost_output DECIMAL(65,18) NULL,cost_currency VARCHAR(16) NULL,
 PRIMARY KEY(org_id,space_id,trace_id,observation_id),KEY idx_statistics_time(org_id,space_id,start_ns)
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin WITH COLUMN GROUP(all columns)`,
	}, nil
}

// TraceStatisticsDDLForVersion keeps OceanBase's explicit row layout while
// seekdb uses its ordinary row-table syntax without the unsupported column-group
// clause. The caller supplies its existing version read; no DDL-error probing.
func TraceStatisticsDDLForVersion(prefix, version string) ([]string, error) {
	statements, err := TraceStatisticsDDL(prefix)
	if err != nil {
		return nil, err
	}
	if !TraceStatisticsVersionSupported(version) {
		return nil, fmt.Errorf("unsupported statistics DDL engine")
	}
	if strings.Contains(strings.ToLower(version), "seekdb") {
		for i, statement := range statements {
			statements[i] = strings.TrimSuffix(statement, " WITH COLUMN GROUP(all columns)")
		}
	}
	return statements, nil
}

func InitializeTraceStatistics(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var version string
	if e := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); e != nil {
		return e
	}
	if !TraceStatisticsVersionSupported(version) {
		return nil
	}
	ready, e := TraceStatisticsSchemaReady(ctx, db, prefix)
	if e != nil || ready {
		return e
	}
	table, e := projectionMaintenanceTable(prefix)
	if e != nil {
		return e
	}
	var generation int64
	if e = db.QueryRowContext(ctx, "SELECT generation FROM "+table+" WHERE id=1 AND enabled=TRUE AND suspended=FALSE AND rebuild_required=FALSE AND schema_version=?", ProjectionSchemaVersion).Scan(&generation); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return nil
		}
		return e
	}
	owner, e := newProjectionCleanupOwner()
	if e != nil {
		return e
	}
	result, e := db.ExecContext(ctx, "UPDATE "+table+" SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+300 WHERE id=1 AND enabled=TRUE AND suspended=FALSE AND rebuild_required=FALSE AND schema_version=? AND generation=? AND migration_until<UNIX_TIMESTAMP()", owner, ProjectionSchemaVersion, generation)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return fmt.Errorf("statistics additive migration lease unavailable")
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		_, _ = db.ExecContext(releaseCtx, "UPDATE "+table+" SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=? AND generation=?", owner, generation)
	}()
	return withProjectionMigrationConnection(ctx, db, func(conn *sql.Conn) error {
		statements, e := TraceStatisticsDDLForVersion(prefix, version)
		if e != nil {
			return e
		}
		for _, statement := range statements {
			if _, e = conn.ExecContext(ctx, statement); e != nil {
				return fmt.Errorf("optional statistics table: %w", e)
			}
		}
		var kind, extra string
		var expression sql.NullString
		e = conn.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_trace_projections", StatisticsReceiptColumn).Scan(&kind, &extra, &expression)
		if e == nil {
			if !expression.Valid || !statisticsReceiptMatches(kind, extra, expression.String) {
				return fmt.Errorf("incompatible optional statistics receipt column")
			}
			ready, e := statisticsTablesReady(ctx, conn, prefix)
			if e != nil {
				return e
			}
			if !ready {
				return fmt.Errorf("incompatible optional statistics table schema")
			}
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		_, e = conn.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" ADD COLUMN "+StatisticsReceiptColumn+" CHAR(64) GENERATED ALWAYS AS ("+StatisticsReceiptExpression+") STORED")
		if e != nil {
			return e
		}
		ready, e := TraceStatisticsSchemaReady(ctx, conn, prefix)
		if e != nil {
			return e
		}
		if !ready {
			return fmt.Errorf("optional statistics schema read-back failed")
		}
		return nil
	})
}
