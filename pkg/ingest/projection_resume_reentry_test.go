package ingest

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func expectResumeSchemaRepair(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"ob_query_timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	for _, suffix := range []string{"trace_sources", "trace_projections", "trace_projection_coverage"} {
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_" + suffix + "`").WillReturnResult(sqlmock.NewResult(0, 0))
	}
	for _, column := range projectionTokenColumns() {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column.name).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow(column.kind, "STORED GENERATED", column.expression))
	}
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectResumeReentryLease(mock sqlmock.Sqlmock, generation int64, version int, ready bool) {
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(ready, generation, version))
	if ready {
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.TABLES")).WithArgs("test_trace_sources", "test_trace_projections", "test_trace_projection_coverage").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
	mock.ExpectQuery("SELECT schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version"}).AddRow(version))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET migration_owner=?,migration_until=UNIX_TIMESTAMP()+300,generation=generation+1,enabled=FALSE,schema_version=0,suspended=FALSE,rebuild_required=TRUE WHERE id=1 AND schema_version<=? AND generation=? AND migration_until<UNIX_TIMESTAMP()")).WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, generation).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func TestResumePartialSchemaZeroAcceptsIntentOnceAndNewOffWins(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	// The preceding interrupted attempt already committed part of its cleanup.
	// Recovery must accept the new intent before any schema repair, once only.
	expectResumeReentryLease(mock, 3, 0, false)
	expectResumeSchemaRepair(mock)
	// Another operator issues OFF during schema repair. Every following fence
	// read sees that newer value, which must survive the entire recovery.
	expectProjectionCleanupChunk(mock, "trace_projections", 4, 17, true)
	expectProjectionCleanupChunk(mock, "trace_sources", 4, 256, true)
	expectProjectionCleanupChunk(mock, "trace_sources", 4, 18, true)
	expectProjectionCleanupChunk(mock, "trace_projection_coverage", 4, 3, true)
	expectProjectionCleanupChunk(mock, "call_statistics", 4, 0, true)
	expectProjectionCleanupChunk(mock, "trace_statistics", 4, 0, true)
	expectProjectionCleanupFinish(mock, 4, true)
	expectProjectionCleanupRelease(mock)
	err := ResumeProjectionMaintenance(context.Background(), db, "test")
	if err == nil || !strings.Contains(err.Error(), "OFF remains active") {
		t.Fatalf("new OFF during schema repair must win, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResumeReadyAndHealthyIsIdempotent(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expectMaintenanceControl(mock)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 7, 1))
	expectHealthyProjectionSchema(mock)
	mock.ExpectCommit()
	if err := ResumeProjectionMaintenance(ctx, db, "test"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResumeReadyWithRemovedDerivedTablesRepairsUnderSingleIntent(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	expectMaintenanceControl(mock)
	expectResumeReentryLease(mock, 7, 1, true)
	expectResumeSchemaRepair(mock)
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		expectProjectionCleanupChunk(mock, suffix, 8, 0, false)
	}
	expectOptionalStatisticsCleanupAbsent(mock, 8)
	expectProjectionCleanupFinish(mock, 8, false)
	expectProjectionCleanupRelease(mock)
	if err := ResumeProjectionMaintenance(ctx, db, "test"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResumeReadySchemaInspectionFailureDoesNotAcceptIntent(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 7, 1))
	metadataErr := errors.New("metadata denied")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.TABLES")).WillReturnError(metadataErr)
	mock.ExpectRollback()
	if err := ResumeProjectionMaintenance(context.Background(), db, "test"); !errors.Is(err, metadataErr) {
		t.Fatalf("must retain metadata error without clearing OFF/intent: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
