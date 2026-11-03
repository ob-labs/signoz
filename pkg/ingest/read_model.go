package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
)

type traceSourceKey struct{ OrgID, SpaceID, TraceID string }

func traceSourceKeys(rows [][]any) []traceSourceKey {
	seen := map[traceSourceKey]bool{}
	for _, row := range rows {
		if len(row) >= 4 {
			seen[traceSourceKey{fmt.Sprint(row[0]), fmt.Sprint(row[1]), fmt.Sprint(row[3])}] = true
		}
	}
	keys := make([]traceSourceKey, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.OrgID != b.OrgID {
			return a.OrgID < b.OrgID
		}
		if a.SpaceID != b.SpaceID {
			return a.SpaceID < b.SpaceID
		}
		return a.TraceID < b.TraceID
	})
	return keys
}

// EnsureTraceReadModelSchema is additive. Raw spans remain the source of truth.
// Standard token columns are part of every new projection table. Existing
// tables are upgraded by the versioned maintenance initializer, never reads.
func EnsureTraceReadModelSchema(ctx context.Context, db *sql.DB, prefix string) error {
	return ensureTraceReadModelSchema(ctx, db, prefix)
}

type projectionSchemaExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func ensureTraceReadModelSchema(ctx context.Context, db projectionSchemaExecutor, prefix string) error {
	if !tablePrefixPattern.MatchString(prefix) {
		return fmt.Errorf("invalid table prefix")
	}
	for _, statement := range []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 org_id VARCHAR(255) NOT NULL, space_id VARCHAR(255) NOT NULL, trace_id VARCHAR(32) NOT NULL,
 source_revision BIGINT UNSIGNED NOT NULL, user_id VARCHAR(255) NOT NULL DEFAULT '', agent_product VARCHAR(255) NOT NULL DEFAULT '', scope_valid BOOLEAN NOT NULL DEFAULT FALSE, min_timestamp BIGINT UNSIGNED NOT NULL,
 max_timestamp BIGINT UNSIGNED NOT NULL, dirty BOOLEAN NOT NULL DEFAULT TRUE,
 PRIMARY KEY(org_id,space_id,trace_id), KEY idx_source_window(org_id,space_id,min_timestamp,max_timestamp),
 KEY idx_source_dirty(org_id,space_id,dirty,trace_id)
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, tableName(prefix, "trace_sources")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 org_id VARCHAR(255) NOT NULL, space_id VARCHAR(255) NOT NULL, trace_id VARCHAR(32) NOT NULL,
 source_revision BIGINT UNSIGNED NOT NULL, semantics_version INT NOT NULL, projection_context CHAR(64) NOT NULL, start_ns BIGINT UNSIGNED NOT NULL, end_ns BIGINT UNSIGNED NULL,
 agent_uuid VARCHAR(255) NOT NULL, agent_product VARCHAR(255) NOT NULL DEFAULT '', user_id VARCHAR(255) NOT NULL DEFAULT '',
 period BIGINT NOT NULL, duration DOUBLE NULL, status SMALLINT NOT NULL, node_count BIGINT NOT NULL,
 environment VARCHAR(255) NULL, scene VARCHAR(255) NOT NULL DEFAULT '', username VARCHAR(255) NULL,
 session_id VARCHAR(255) NOT NULL DEFAULT '', name VARCHAR(512) NOT NULL DEFAULT '', model VARCHAR(255) NOT NULL DEFAULT '',
 search_text LONGTEXT NOT NULL, input_text LONGTEXT NOT NULL, output_text LONGTEXT NOT NULL,
 scene_values_json LONGTEXT NOT NULL, tags_json LONGTEXT NOT NULL, row_json LONGTEXT NOT NULL, preview_json LONGTEXT NOT NULL,
 session_trace_tokens_v1 DECIMAL(65,0) GENERATED ALWAYS AS (%s) STORED,
 session_billable_v1 TINYINT GENERATED ALWAYS AS (%s) STORED,
 PRIMARY KEY(org_id,space_id,trace_id),
 KEY idx_projection_agent_period(org_id,space_id,agent_uuid,start_ns),
 KEY idx_projection_agent_environment(org_id,space_id,environment,period),
 KEY idx_projection_agent_username(org_id,space_id,username,period)
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, tableName(prefix, "trace_projections"), SessionTokenGeneratedValueExpr, SessionTokenGeneratedBillableExpr),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
 org_id VARCHAR(255) NOT NULL, space_id VARCHAR(255) NOT NULL, after_trace_id VARCHAR(32) NOT NULL DEFAULT '',
 complete BOOLEAN NOT NULL DEFAULT FALSE, PRIMARY KEY(org_id,space_id)
 ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, tableName(prefix, "trace_projection_coverage")),
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ensure trace read model: %w", err)
		}
	}
	return nil
}

func (ep *endpoint) dirtyTraceSources(ctx context.Context, tx *sql.Tx, rows [][]any) error {
	query := `INSERT INTO ` + tableName(ep.cfg.TablePrefix, "trace_sources") + ` (org_id,space_id,trace_id,source_revision,user_id,agent_product,scope_valid,min_timestamp,max_timestamp,dirty)
 SELECT org_id,space_id,trace_id,1,MIN(user_id),MIN(agent_product),(COUNT(DISTINCT BINARY user_id)=1 AND MIN(user_id)<>'' AND COUNT(DISTINCT BINARY agent_product)=1 AND MIN(agent_product)<>''),MIN(timestamp),MAX(timestamp),TRUE FROM ` + ep.tracesTable + ` WHERE org_id=? AND space_id=? AND trace_id=? GROUP BY org_id,space_id,trace_id
 ON DUPLICATE KEY UPDATE source_revision=source_revision+1,user_id=VALUES(user_id),agent_product=VALUES(agent_product),scope_valid=VALUES(scope_valid),min_timestamp=VALUES(min_timestamp),max_timestamp=VALUES(max_timestamp),dirty=TRUE`
	for _, key := range traceSourceKeys(rows) {
		if _, err := tx.ExecContext(ctx, query, key.OrgID, key.SpaceID, key.TraceID); err != nil {
			return fmt.Errorf("dirty trace source: %w", err)
		}
	}
	return nil
}

// LockTraceSource serializes ingest, reconciliation and CAS publication before
// any raw trace writes. New placeholder rows are invisible until commit.
func LockTraceSource(ctx context.Context, tx *sql.Tx, prefix, org, space, trace string) error {
	if !tablePrefixPattern.MatchString(prefix) {
		return fmt.Errorf("invalid table prefix")
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+tableName(prefix, "trace_sources")+" (org_id,space_id,trace_id,source_revision,min_timestamp,max_timestamp,dirty) VALUES (?,?,?,0,0,0,TRUE) ON DUPLICATE KEY UPDATE trace_id=VALUES(trace_id)", org, space, trace)
	return err
}

// InitializeTraceReadModelTracking is retained as an internal test/bootstrap
// helper. Unlike the old implementation, ordinary restarts retain coverage.
func InitializeTraceReadModelTracking(ctx context.Context, db *sql.DB, prefix string) error {
	return InitializeTraceReadModelMaintenance(ctx, db, prefix, true)
}
