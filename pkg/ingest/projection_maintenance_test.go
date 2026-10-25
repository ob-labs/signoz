package ingest

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func TestProjectionMaintenanceDefaultsAndEmergencyControl(t *testing.T) {
	for _, test := range []struct {
		value   string
		enabled bool
	}{{"", true}, {"true", true}, {"false", false}, {"FALSE", false}, {"0", false}, {"garbage", false}} {
		t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", test.value)
		if ProjectionMaintenanceEnabled() != test.enabled {
			t.Fatalf("%q: wrong maintenance default", test.value)
		}
	}
}

func TestProjectionFenceMissingAndFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantError bool
	}{{"absent", sql.ErrNoRows, false}, {"unmigrated", &mysql.MySQLError{Number: 1146}, false}, {"permission", &mysql.MySQLError{Number: 1142}, true}, {"transport", errors.New("offline"), true}} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, _ := sqlmock.New()
			defer db.Close()
			mock.ExpectBegin()
			tx, _ := db.Begin()
			mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnError(test.err)
			enabled, _, err := ReadProjectionMaintenance(context.Background(), tx, "test")
			if enabled || (err != nil) != test.wantError {
				t.Fatalf("enabled=%v err=%v", enabled, err)
			}
			mock.ExpectRollback()
			_ = tx.Rollback()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProjectionFenceSchemaVersionAndOffLatch(t *testing.T) {
	for _, test := range []struct {
		enabled bool
		version int
		ready   bool
	}{{true, 1, true}, {true, 0, false}, {false, 1, false}} {
		db, mock, _ := sqlmock.New()
		mock.ExpectBegin()
		tx, _ := db.Begin()
		mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(test.enabled, 7, test.version))
		enabled, generation, err := ReadProjectionMaintenance(context.Background(), tx, "test")
		if err != nil || enabled != test.ready || generation != 7 {
			t.Fatalf("enabled=%v generation=%v err=%v", enabled, generation, err)
		}
		mock.ExpectRollback()
		_ = tx.Rollback()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestResumeProjectionMaintenanceRebuildsDerivedOnly(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectHealthyMaintenanceInitialization(mock)
	expectResumeCleanupLease(mock, 2)
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		expectProjectionCleanupChunk(mock, suffix, 3, 20, false)
	}
	expectProjectionCleanupFinish(mock, 3, false)
	expectProjectionCleanupRelease(mock)
	if err := ResumeProjectionMaintenance(context.Background(), db, "test"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResumeDoesNotDeleteWhenSchemaRepairFails(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectQuery("SELECT schema_version,generation FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(ProjectionSchemaVersion, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.TABLES")).WillReturnError(errors.New("metadata permission denied"))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET generation=generation+IF(enabled,1,0),enabled=FALSE,schema_version=0,rebuild_required=TRUE WHERE id=1 AND schema_version=? AND generation=?")).WithArgs(ProjectionSchemaVersion, int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := ResumeProjectionMaintenance(context.Background(), db, "test"); err == nil {
		t.Fatal("schema inspection failure must stop resume before derived deletes")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectMaintenanceControl(mock sqlmock.Sqlmock) {
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_projection_maintenance`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO `test_projection_maintenance`").WillReturnResult(sqlmock.NewResult(0, 0))
}

func TestOrdinaryProjectionRestartPreservesCoverageAndOffLatch(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectHealthyMaintenanceInitialization(mock)
	if err := InitializeTraceReadModelMaintenance(context.Background(), db, "test", true); err != nil {
		t.Fatal(err)
	}
	// No reset, backfill-cursor write or implicit re-enable is permitted.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionMigrationFailureKeepsRawEligibleAndDerivedFenced(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectQuery("SELECT schema_version,generation FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(0, 1))
	mock.ExpectExec("UPDATE `test_projection_maintenance` SET migration_owner").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"ob_query_timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_trace_sources`").WillReturnError(errors.New("DDL permission denied"))
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE `test_projection_maintenance` SET migration_owner='',migration_until=0 WHERE id=1 AND migration_owner").WithArgs(sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := InitializeTraceReadModelMaintenance(context.Background(), db, "test", true); err == nil {
		t.Fatal("must report failed migration")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentProjectionMigratorDoesNotRunDDL(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectQuery("SELECT schema_version,generation FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(0, 1))
	mock.ExpectExec("UPDATE `test_projection_maintenance` SET migration_owner").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(1)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := InitializeTraceReadModelMaintenance(context.Background(), db, "test", true); err == nil {
		t.Fatal("concurrent migrator must defer until completed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceOffSkipsDerivedSchemaAndLatches(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 2, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET generation=generation+IF(enabled,1,0),enabled=FALSE,suspended=TRUE,rebuild_required=TRUE")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := InitializeTraceReadModelMaintenance(context.Background(), db, "test", false); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestResumeFailurePreservesOffAndCommittedCleanupProgress(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectHealthyMaintenanceInitialization(mock)
	expectResumeCleanupLease(mock, 2)
	expectProjectionCleanupChunk(mock, "trace_projections", 3, 20, false)
	mock.ExpectBegin()
	expectProjectionCleanupFence(mock, sqlmock.AnyArg(), 3, false)
	mock.ExpectExec("DELETE FROM `test_trace_sources` LIMIT 256").WillReturnError(errors.New("delete failed"))
	mock.ExpectRollback()
	expectProjectionCleanupRelease(mock)
	if err := ResumeProjectionMaintenance(context.Background(), db, "test"); err == nil {
		t.Fatal("resume must stay OFF after partially committed derived cleanup")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionMigrationRestoresDedicatedSQLDeadline(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"ob_query_timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
	err := withProjectionMigrationConnection(context.Background(), db, func(*sql.Conn) error { return errors.New("DDL failed") })
	if err == nil || err.Error() != "DDL failed" {
		t.Fatalf("lost migration failure: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionMigrationRejectsLeakedDeadline(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"ob_query_timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnError(errors.New("reset failed"))
	if err := withProjectionMigrationConnection(context.Background(), db, func(*sql.Conn) error { return nil }); err == nil {
		t.Fatal("failed connection reset cannot count as successful migration")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionTokenMigrationResumesAfterPartialDDL(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", SessionTokenGeneratedValueColumn).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow("decimal(65,0)", "STORED GENERATED", SessionTokenGeneratedValueExpr))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", SessionTokenGeneratedBillableColumn).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE `test_trace_projections` ADD COLUMN " + SessionTokenGeneratedBillableColumn + " TINYINT GENERATED ALWAYS AS (" + SessionTokenGeneratedBillableExpr + ") STORED")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := migrateSessionTokenColumns(context.Background(), db, "test"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionTokenMigrationRepairsExpressionDrift(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", SessionTokenGeneratedValueColumn).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow("decimal(65,0)", "STORED GENERATED", "0"))
	mock.ExpectExec("ALTER TABLE `test_trace_projections` DROP COLUMN " + SessionTokenGeneratedValueColumn).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ALTER TABLE `test_trace_projections` ADD COLUMN " + SessionTokenGeneratedValueColumn).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", SessionTokenGeneratedBillableColumn).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow("tinyint(4)", "STORED GENERATED", SessionTokenGeneratedBillableExpr))
	if err := migrateSessionTokenColumns(context.Background(), db, "test"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectHealthyMaintenanceInitialization(mock sqlmock.Sqlmock) {
	expectMaintenanceControl(mock)
	mock.ExpectQuery("SELECT schema_version,generation FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(ProjectionSchemaVersion, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.TABLES")).WithArgs("test_trace_sources", "test_trace_projections", "test_trace_projection_coverage").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	for _, column := range projectionTokenColumns() {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column.name).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow(column.kind, "STORED GENERATED", column.expression))
	}
}

func TestLateSchemaProbeErrorDoesNotInvalidateAdvancedGeneration(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	expectMaintenanceControl(mock)
	mock.ExpectQuery("SELECT schema_version,generation FROM `test_projection_maintenance`").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(ProjectionSchemaVersion, 7))
	probeErr := errors.New("delayed schema metadata failure")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.TABLES")).WillReturnError(probeErr)
	// Another replica repaired the schema and advanced generation before this
	// delayed error arrived. The observed-generation CAS affects no current row.
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET generation=generation+IF(enabled,1,0),enabled=FALSE,schema_version=0,rebuild_required=TRUE WHERE id=1 AND schema_version=? AND generation=?")).WithArgs(ProjectionSchemaVersion, int64(7)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := InitializeTraceReadModelMaintenance(context.Background(), db, "test", true); !errors.Is(err, probeErr) {
		t.Fatalf("must preserve actual metadata error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
