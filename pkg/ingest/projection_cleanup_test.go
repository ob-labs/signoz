package ingest

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func expectResumeCleanupLease(mock sqlmock.Sqlmock, generation int64) {
	expectResumeReentryLease(mock, generation, ProjectionSchemaVersion, false)
}

func expectProjectionCleanupFence(mock sqlmock.Sqlmock, owner any, generation int64, suspended bool) {
	mock.ExpectQuery(regexp.QuoteMeta("SELECT suspended FROM `test_projection_maintenance` WHERE id=1 AND migration_owner=? AND generation=? AND enabled=FALSE AND schema_version=0 FOR UPDATE")).WithArgs(owner, generation).WillReturnRows(sqlmock.NewRows([]string{"suspended"}).AddRow(suspended))
}

func expectProjectionCleanupChunk(mock sqlmock.Sqlmock, suffix string, generation, affected int64, suspended bool) {
	mock.ExpectBegin()
	expectProjectionCleanupFence(mock, sqlmock.AnyArg(), generation, suspended)
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `test_" + suffix + "` LIMIT 256")).WillReturnResult(sqlmock.NewResult(0, affected))
	mock.ExpectCommit()
}

func expectProjectionCleanupFinish(mock sqlmock.Sqlmock, generation int64, suspended bool) {
	mock.ExpectBegin()
	expectProjectionCleanupFence(mock, sqlmock.AnyArg(), generation, suspended)
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET schema_version=?,enabled=?,rebuild_required=FALSE,migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=? AND generation=?")).WithArgs(ProjectionSchemaVersion, !suspended, sqlmock.AnyArg(), generation).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func expectProjectionCleanupRelease(mock sqlmock.Sqlmock) {
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner=?")).WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestProjectionCleanupStopsBeforeDeleteOnLostLeaseOrGeneration(t *testing.T) {
	for _, test := range []struct {
		name, owner string
		generation  int64
	}{{"lease", "old-owner", 7}, {"generation", "owner", 6}} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT suspended FROM `test_projection_maintenance`").WithArgs(test.owner, test.generation).WillReturnError(sql.ErrNoRows)
			mock.ExpectRollback()
			if _, err := deleteProjectionDerivedChunk(context.Background(), db, "test", "trace_projections", projectionCleanupLease{test.owner, test.generation}); err == nil {
				t.Fatal("lost lease/generation must stop cleanup")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProjectionCleanupCommitsBoundedChunksAndRemainingRowsResume(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 256, false)
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 17, false)
	expectProjectionCleanupChunk(mock, "trace_sources", 3, 0, false)
	expectProjectionCleanupChunk(mock, "trace_projection_coverage", 3, 0, false)
	if err := resetProjectionDerived(context.Background(), db, "test", projectionCleanupLease{"owner", 3}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRawWriterCanCommitBetweenProjectionCleanupChunks(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	lease := projectionCleanupLease{"owner", 3}
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 256, false)
	if _, err := deleteProjectionDerivedChunk(context.Background(), db, "test", "trace_projections", lease); err != nil {
		t.Fatal(err)
	}
	// Raw ingestion locks the same durable fence after the chunk committed.
	// It remains available without tracking against a half-cleaned derived model.
	ep := &endpoint{db: db, cfg: Config{TablePrefix: "test"}, tracesTable: "`test_traces`"}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(false, 3, 0))
	mock.ExpectExec("UPDATE `test_projection_maintenance` SET enabled=FALSE,rebuild_required=TRUE").WillReturnResult(sqlmock.NewResult(0, 1))
	prepared := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `test_traces` (x) VALUES (?)"))
	prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := ep.insertRows(context.Background(), ep.tracesTable, " (x) VALUES (?)", "", [][]any{{"org", "space", 1, "abc"}}); err != nil {
		t.Fatal(err)
	}
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 0, false)
	if _, err := deleteProjectionDerivedChunk(context.Background(), db, "test", "trace_projections", lease); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewOffDuringResumeIsNotOverriddenOrReportedResumed(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	expectResumeCleanupLease(mock, 2)
	expectResumeSchemaRepair(mock)
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		expectProjectionCleanupChunk(mock, suffix, 3, 0, false)
	}
	// A newer OFF changed suspended while reads were still disabled. Finalize
	// must retain that OFF value and report that recovery did not resume reads.
	expectProjectionCleanupFinish(mock, 3, true)
	expectProjectionCleanupRelease(mock)
	if err := ResumeProjectionMaintenance(context.Background(), db, "test"); err == nil {
		t.Fatal("newer OFF must not produce resumed-success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedChunkKeepsPreviouslyCommittedProgressAndSkipsEnable(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 256, false)
	mock.ExpectBegin()
	expectProjectionCleanupFence(mock, "owner", 3, false)
	mock.ExpectExec("DELETE FROM `test_trace_projections` LIMIT 256").WillReturnError(errors.New("temporary SQL failure"))
	mock.ExpectRollback()
	if err := resetProjectionDerived(context.Background(), db, "test", projectionCleanupLease{"owner", 3}); err == nil {
		t.Fatal("failed chunk must stop before enabling projections")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
