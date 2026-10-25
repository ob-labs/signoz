package ingest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// Cleanup is derived-only. Short commits keep the ordinary statement deadline
// and let raw writers acquire the shared fence between chunks. The durable OFF
// state, generation and migration lease were committed before cleanup starts.
const projectionCleanupChunkSize = 256

type projectionCleanupLease struct {
	owner      string
	generation int64
}

func newProjectionCleanupOwner() (string, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return hex.EncodeToString(token), nil
}

func projectionCleanupState(ctx context.Context, tx *sql.Tx, prefix string, lease projectionCleanupLease) (bool, error) {
	var suspended bool
	err := tx.QueryRowContext(ctx, "SELECT suspended FROM "+tableName(prefix, "projection_maintenance")+" WHERE id=1 AND migration_owner=? AND generation=? AND enabled=FALSE AND schema_version=0 FOR UPDATE", lease.owner, lease.generation).Scan(&suspended)
	if err == sql.ErrNoRows {
		return false, fmt.Errorf("projection cleanup lease or generation changed")
	}
	return suspended, err
}

func deleteProjectionDerivedChunk(ctx context.Context, db *sql.DB, prefix, suffix string, lease projectionCleanupLease) (int64, error) {
	if _, err := projectionMaintenanceTable(prefix); err != nil {
		return 0, err
	}
	switch suffix {
	case "trace_projections", "trace_sources", "trace_projection_coverage":
	default:
		return 0, fmt.Errorf("invalid projection cleanup table")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := projectionCleanupState(ctx, tx, prefix, lease); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM "+tableName(prefix, suffix)+" LIMIT 256")
	if err != nil {
		return 0, fmt.Errorf("delete projection cleanup chunk %s: %w", suffix, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected < 0 || affected > projectionCleanupChunkSize {
		return 0, fmt.Errorf("invalid projection cleanup affected row count")
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit projection cleanup chunk %s: %w", suffix, err)
	}
	return affected, nil
}

func resetProjectionDerived(ctx context.Context, db *sql.DB, prefix string, lease projectionCleanupLease) error {
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			affected, err := deleteProjectionDerivedChunk(ctx, db, prefix, suffix, lease)
			if err != nil {
				return err
			}
			if affected < projectionCleanupChunkSize {
				break
			}
		}
	}
	return nil
}

func finishProjectionCleanup(ctx context.Context, db *sql.DB, prefix string, lease projectionCleanupLease, explicitResume bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	suspended, err := projectionCleanupState(ctx, tx, prefix, lease)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE "+tableName(prefix, "projection_maintenance")+" SET schema_version=?,enabled=?,rebuild_required=FALSE,migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=? AND generation=?", ProjectionSchemaVersion, !suspended, lease.owner, lease.generation)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("projection cleanup lost its lease during finalization")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if explicitResume && suspended {
		return fmt.Errorf("projection maintenance was suspended during recovery; OFF remains active")
	}
	return nil
}

func releaseProjectionCleanupLease(ctx context.Context, db *sql.DB, prefix string, lease projectionCleanupLease) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	// Clear only the nonce we acquired. Do not overwrite eligibility or another
	// generation when an attempt fails after an operator or another replica acts.
	_, _ = db.ExecContext(releaseCtx, "UPDATE "+tableName(prefix, "projection_maintenance")+" SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=?", lease.owner)
}
