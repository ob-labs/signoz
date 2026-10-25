package ingest

// The durable fence is deliberately separate from the derived tables. Turning
// maintenance off must remain possible when a projection migration has failed.
// Every trace write locks this row before either tracking or committing raw
// data. Readers observe the same row in their repeatable-read snapshot.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const ProjectionSchemaVersion = 1
const SessionTokenGeneratedValueColumn = "session_trace_tokens_v1"
const SessionTokenGeneratedBillableColumn = "session_billable_v1"
const SessionTokenGeneratedValueExpr = "CASE WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')=0 THEN 0 WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')>0 AND JSON_TYPE(JSON_EXTRACT(row_json,'$.usage.total')) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') THEN CAST(JSON_UNQUOTE(JSON_EXTRACT(row_json,'$.usage.total')) AS DECIMAL(65,0)) ELSE NULL END"
const SessionTokenGeneratedBillableExpr = "CASE WHEN JSON_EXTRACT(row_json,'$.coverage.billableCalls')>0 THEN 1 ELSE 0 END"

func ProjectionMaintenanceEnabled() bool {
	value := strings.TrimSpace(os.Getenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED"))
	if value == "" {
		return true
	}
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}

func projectionMaintenanceTable(prefix string) (string, error) {
	if !tablePrefixPattern.MatchString(prefix) {
		return "", fmt.Errorf("invalid table prefix")
	}
	return tableName(prefix, "projection_maintenance"), nil
}

func projectionMaintenanceState(ctx context.Context, tx *sql.Tx, prefix string, lock bool) (bool, int64, error) {
	table, err := projectionMaintenanceTable(prefix)
	if err != nil {
		return false, 0, err
	}
	query := "SELECT enabled,generation,schema_version FROM " + table + " WHERE id=1"
	if lock {
		query += " FOR UPDATE"
	}
	var enabled bool
	var generation int64
	var version int
	err = tx.QueryRowContext(ctx, query).Scan(&enabled, &generation, &version)
	var databaseError *mysql.MySQLError
	if errors.Is(err, sql.ErrNoRows) || errors.As(err, &databaseError) && databaseError.Number == 1146 {
		return false, 0, nil
	}
	if err != nil {
		return false, 0, err
	}
	return enabled && version == ProjectionSchemaVersion, generation, nil
}

func ReadProjectionMaintenance(ctx context.Context, tx *sql.Tx, prefix string) (bool, int64, error) {
	return projectionMaintenanceState(ctx, tx, prefix, false)
}
func LockProjectionMaintenance(ctx context.Context, tx *sql.Tx, prefix string) (bool, int64, error) {
	return projectionMaintenanceState(ctx, tx, prefix, true)
}

func ensureProjectionMaintenanceControl(ctx context.Context, db *sql.DB, prefix string) error {
	table, err := projectionMaintenanceTable(prefix)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+` (
 id TINYINT NOT NULL PRIMARY KEY, enabled BOOLEAN NOT NULL DEFAULT FALSE,
 suspended BOOLEAN NOT NULL DEFAULT FALSE, generation BIGINT NOT NULL DEFAULT 1,
 schema_version INT NOT NULL DEFAULT 0, rebuild_required BOOLEAN NOT NULL DEFAULT TRUE,
 migration_owner CHAR(32) NOT NULL DEFAULT '', migration_until BIGINT NOT NULL DEFAULT 0
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	if err != nil {
		return fmt.Errorf("ensure projection maintenance fence: %w", err)
	}
	_, err = db.ExecContext(ctx, "INSERT IGNORE INTO "+table+" (id) VALUES (1)")
	return err
}

// SuspendProjectionMaintenance is a sticky fleet-wide OFF latch. Starting an
// older/default-on replica cannot undo it. Only the explicit resume action can
// rebuild and re-enable a suspended model.
func SuspendProjectionMaintenance(ctx context.Context, db *sql.DB, prefix string) error {
	table, err := projectionMaintenanceTable(prefix)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = LockProjectionMaintenance(ctx, tx, prefix); err != nil {
		return err
	}
	// MySQL evaluates assignments left to right. Read the old enabled value
	// before assigning FALSE so suspension advances the generation exactly once.
	_, err = tx.ExecContext(ctx, "UPDATE "+table+" SET generation=generation+IF(enabled,1,0),enabled=FALSE,suspended=TRUE,rebuild_required=TRUE WHERE id=1")
	if err != nil {
		return err
	}
	return tx.Commit()
}

func ResumeProjectionMaintenance(ctx context.Context, db *sql.DB, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// Initialization repairs a full derived-schema rollback without clearing OFF.
	if err := InitializeTraceReadModelMaintenance(ctx, db, prefix, true); err != nil {
		return err
	}
	table, err := projectionMaintenanceTable(prefix)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ready, generation, err := LockProjectionMaintenance(ctx, tx, prefix)
	if err != nil {
		return err
	}
	if ready {
		return tx.Commit()
	}
	var version int
	if err := tx.QueryRowContext(ctx, "SELECT schema_version FROM "+table+" WHERE id=1").Scan(&version); err != nil {
		return err
	}
	if version != ProjectionSchemaVersion {
		return fmt.Errorf("projection schema migration is incomplete")
	}
	owner, err := newProjectionCleanupOwner()
	if err != nil {
		return err
	}
	// Persist the authorized resume intent while reads remain disabled. A newer
	// operator OFF or an OFF writer can set suspended=TRUE between cleanup chunks.
	result, err := tx.ExecContext(ctx, "UPDATE "+table+" SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+300,generation=generation+1,enabled=FALSE,schema_version=0,suspended=FALSE,rebuild_required=TRUE WHERE id=1 AND schema_version=? AND generation=? AND migration_until<UNIX_TIMESTAMP()", owner, ProjectionSchemaVersion, generation)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("projection maintenance recovery is already in progress")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	lease := projectionCleanupLease{owner, generation + 1}
	defer releaseProjectionCleanupLease(ctx, db, prefix, lease)
	if err := resetProjectionDerived(ctx, db, prefix, lease); err != nil {
		return err
	}
	return finishProjectionCleanup(ctx, db, prefix, lease, true)
}

// InitializeTraceReadModelMaintenance performs the versioned migration once.
// The short, renewable-by-retry migration lease serializes DDL without relying
// on MySQL advisory locks, which are not universal across supported engines.
// Startup's context must be shorter than the five-minute lease. A crashed
// migrator is recoverable after the lease; each DDL step inspects actual state.
func InitializeTraceReadModelMaintenance(ctx context.Context, db *sql.DB, prefix string, enabled bool) error {
	// Enforce the lease bound here as well as at startup callers. A helper or
	// future endpoint must not keep running DDL after another migrator may own
	// the expired five-minute lease.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if err := ensureProjectionMaintenanceControl(ctx, db, prefix); err != nil {
		return err
	}
	if !enabled {
		return SuspendProjectionMaintenance(ctx, db, prefix)
	}
	table, _ := projectionMaintenanceTable(prefix)
	var version int
	var observedGeneration int64
	if err := db.QueryRowContext(ctx, "SELECT schema_version,generation FROM "+table+" WHERE id=1").Scan(&version, &observedGeneration); err != nil {
		return err
	}
	if version == ProjectionSchemaVersion {
		ready, err := projectionSchemaReady(ctx, db, prefix)
		if err != nil {
			// An unreadable schema cannot establish read eligibility. Keep the raw
			// service available and let the bounded initializer retry the inspection.
			_, invalidateErr := db.ExecContext(ctx, "UPDATE "+table+" SET generation=generation+IF(enabled,1,0),enabled=FALSE,schema_version=0,rebuild_required=TRUE WHERE id=1 AND schema_version=? AND generation=?", version, observedGeneration)
			return errors.Join(err, invalidateErr)
		}
		if ready {
			return nil
		}
	}
	if version > ProjectionSchemaVersion {
		return fmt.Errorf("projection schema is newer than this binary")
	}
	owner, err := newProjectionCleanupOwner()
	if err != nil {
		return err
	}
	result, err := db.ExecContext(ctx, "UPDATE "+table+" SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+300,generation=generation+1,enabled=FALSE,schema_version=0,rebuild_required=TRUE WHERE id=1 AND schema_version<=? AND generation=? AND migration_until<UNIX_TIMESTAMP()", owner, ProjectionSchemaVersion, observedGeneration)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("projection schema migration is already in progress; raw queries remain available")
	}
	// Eligibility and the generation were durably changed before any DDL or
	// cleanup. A failed attempt releases only its matching lease nonce; already
	// committed cleanup chunks remain a safe, resumable checkpoint.
	lease := projectionCleanupLease{owner, observedGeneration + 1}
	defer releaseProjectionCleanupLease(ctx, db, prefix, lease)
	if err := withProjectionMigrationConnection(ctx, db, func(conn *sql.Conn) error {
		if err := ensureTraceReadModelSchema(ctx, conn, prefix); err != nil {
			return err
		}
		return migrateSessionTokenColumns(ctx, conn, prefix)
	}); err != nil {
		return err
	}
	if err := resetProjectionDerived(ctx, db, prefix, lease); err != nil {
		return err
	}
	return finishProjectionCleanup(ctx, db, prefix, lease, false)
}

type projectionSchemaConnection interface {
	projectionSchemaExecutor
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Only the dedicated DDL connection receives the longer SQL deadline. Restore
// its prior session setting before returning it to the ordinary query pool.
func withProjectionMigrationConnection(ctx context.Context, db *sql.DB, migrate func(*sql.Conn) error) (err error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var previous int64
	if err := conn.QueryRowContext(ctx, "SELECT @@session.ob_query_timeout").Scan(&previous); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION ob_query_timeout=120000000"); err != nil {
		return err
	}
	defer func() {
		restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, restoreErr := conn.ExecContext(restoreCtx, fmt.Sprintf("SET SESSION ob_query_timeout=%d", previous)); restoreErr != nil {
			// A failed reset must never leak the migration deadline into raw/query work.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			if err == nil {
				err = fmt.Errorf("restore projection migration session timeout: %w", restoreErr)
			}
		}
	}()
	return migrate(conn)
}

type projectionTokenColumn struct{ name, kind, expression string }

func projectionTokenColumns() []projectionTokenColumn {
	return []projectionTokenColumn{{SessionTokenGeneratedValueColumn, "DECIMAL(65,0)", SessionTokenGeneratedValueExpr}, {SessionTokenGeneratedBillableColumn, "TINYINT", SessionTokenGeneratedBillableExpr}}
}

func projectionTokenColumnReady(ctx context.Context, db projectionSchemaConnection, prefix string, column projectionTokenColumn) (bool, bool, error) {
	var kind, extra string
	var expression sql.NullString
	err := db.QueryRowContext(ctx, "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_trace_projections", column.name).Scan(&kind, &extra, &expression)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	validKind := strings.EqualFold(kind, column.kind) || (column.kind == "TINYINT" && strings.EqualFold(kind, "tinyint(4)"))
	valid := validKind && strings.ToUpper(strings.Join(strings.Fields(extra), " ")) == "STORED GENERATED" && expression.Valid && SessionTokenGeneratedExpressionMatches(expression.String, column.expression)
	return valid, true, nil
}

// Metadata is rechecked on startup/recovery, even at the current version. A
// version record is not proof that an emergency rollback left the tables in
// place. Healthy restarts perform no DDL, reset or eligibility changes.
func projectionSchemaReady(ctx context.Context, db projectionSchemaConnection, prefix string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN (?,?,?)", prefix+"_trace_sources", prefix+"_trace_projections", prefix+"_trace_projection_coverage").Scan(&count); err != nil {
		return false, err
	}
	if count != 3 {
		return false, nil
	}
	for _, column := range projectionTokenColumns() {
		ready, _, err := projectionTokenColumnReady(ctx, db, prefix, column)
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}
	}
	return true, nil
}

func migrateSessionTokenColumns(ctx context.Context, db projectionSchemaConnection, prefix string) error {
	for _, column := range projectionTokenColumns() {
		ready, exists, err := projectionTokenColumnReady(ctx, db, prefix, column)
		if err != nil {
			return err
		}
		if ready {
			continue
		}
		// These two versioned columns are database-derived helpers. Repair their
		// schema only after the shared migration lease has durably revoked reads
		// and advanced the generation. row_json and every raw span remain intact.
		if exists {
			if _, err := db.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" DROP COLUMN "+column.name); err != nil {
				return fmt.Errorf("replace projection token column %s: %w", column.name, err)
			}
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE "+tableName(prefix, "trace_projections")+" ADD COLUMN "+column.name+" "+column.kind+" GENERATED ALWAYS AS ("+column.expression+") STORED"); err != nil {
			return fmt.Errorf("migrate projection token column %s: %w", column.name, err)
		}
	}
	return nil
}

// Compare a narrow SQL token stream, preserving literal bytes and operator /
// parenthesis order while accepting the engines' documented NUMBER rendering.
func SessionTokenGeneratedExpressionMatches(actual, source string) bool {
	normalize := func(value string) string {
		var out strings.Builder
		quoted := false
		for _, c := range value {
			if c == '\'' {
				quoted = !quoted
				out.WriteRune(c)
			} else if quoted {
				out.WriteRune(c)
			} else if c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '`' {
				out.WriteString(strings.ToLower(string(c)))
			}
		}
		return strings.ReplaceAll(out.String(), "asnumber(", "asdecimal(")
	}
	a, s := normalize(actual), normalize(source)
	if a == s || a == "("+s+")" {
		return true
	}
	// Parenthesized expression form reported by OceanBase CE and seekdb.
	stored := map[string]string{
		SessionTokenGeneratedValueExpr:    "(case when (JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') = 0) then 0 when ((JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') > 0) and (JSON_TYPE(JSON_EXTRACT(`row_json`,'$.usage.total')) in ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL'))) then cast(JSON_UNQUOTE(JSON_EXTRACT(`row_json`,'$.usage.total')) as number(65,0)) else NULL end)",
		SessionTokenGeneratedBillableExpr: "(case when (JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') > 0) then 1 else 0 end)",
	}
	expected, ok := stored[source]
	return ok && a == normalize(expected)
}
