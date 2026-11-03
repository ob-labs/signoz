package oceanbasetelemetrystore

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

func initializeModelsForLifecycle(ctx context.Context, db *sql.DB) (bool, error) {
	return initializeProjectionModels(ctx, db, "test")
}

func retryModelsForLifecycle(ctx context.Context, retry, idle time.Duration, initialize func(context.Context) (bool, error)) {
	retryProjectionMaintenance(ctx, retry, idle, initialize)
}

func expectLifecycleBaseReady(mock sqlmock.Sqlmock, generation int64) {
	// Existing idempotent control-table ensure is retained; this fixture permits
	// no base-index rebuild, generated-column DDL or optional DDL while OFF.
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT schema_version,generation FROM").WillReturnRows(sqlmock.NewRows([]string{"schema_version", "generation"}).AddRow(1, generation))
	mock.ExpectQuery("SELECT COUNT.*information_schema.TABLES").WithArgs("test_trace_sources", "test_trace_projections", "test_trace_projection_coverage").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.SessionTokenGeneratedValueColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("decimal(65,0)", "STORED GENERATED", ingest.SessionTokenGeneratedValueExpr))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.SessionTokenGeneratedBillableColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("tinyint(4)", "STORED GENERATED", ingest.SessionTokenGeneratedBillableExpr))
}

func expectLifecycleSummariesReady(mock sqlmock.Sqlmock) {
	for phase := 0; phase < 2; phase++ {
		columns := [][2]string{{ingest.TraceProjectionMetadataColumn, ingest.TraceProjectionMetadataExpression}, {ingest.TraceProjectionContentColumn, ingest.TraceProjectionContentExpression}}
		if phase == 0 {
			columns = append(columns, [2]string{ingest.TraceProjectionMetricsVersionColumn, ingest.TraceProjectionMetricsVersionExpression}, [2]string{ingest.TraceProjectionOperationPreviewsColumn, ingest.TraceProjectionOperationPreviewsExpression}, [2]string{ingest.TraceProjectionMetricsColumn, ingest.TraceProjectionMetricsExpression}, [2]string{ingest.TraceProjectionRowKeysColumn, ingest.TraceProjectionRowKeysExpression})
		}
		for _, column := range columns {
			mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
		}
	}
	for _, column := range [][2]string{{ingest.TraceProjectionMetadataColumn, ingest.TraceProjectionMetadataExpression}, {ingest.TraceProjectionMetricsColumn, ingest.TraceProjectionMetricsExpression}, {ingest.TraceProjectionRowKeysColumn, ingest.TraceProjectionRowKeysExpression}, {ingest.TraceProjectionOperationPreviewsColumn, ingest.TraceProjectionOperationPreviewsExpression}} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
	}
}

func expectLifecycleContentReady(mock sqlmock.Sqlmock) {
	expectLifecycleSummariesReady(mock)
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceContentCompleteColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("tinyint(4)", "STORED GENERATED", ingest.TraceContentCompleteExpression))
}

func TestProjectionLifecycleContentFailureDoesNotBlockReadyStatistics(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleBaseReady(mock, 7)
	expectLifecycleVersion(mock)
	// Full existing statistics type/receipt/primary-key proof is consumed
	// before an unrelated optional content definition is found incompatible.
	expectLifecycleStatisticsReady(mock, true)
	expectLifecycleSummariesReady(mock)
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceContentCompleteColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("tinyint", "", "1"))
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.ErrorContains(t, err, "incompatible optional content-completeness column")
	require.False(t, complete, "the worker must retry content readiness without invalidating statistics")
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectLifecycleVersion(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase seekdb-v1.4.0.0"))
}

func expectLifecycleStatisticsReady(mock sqlmock.Sqlmock, ready bool) {
	count := 0
	if ready {
		count = 2
	}
	mock.ExpectQuery("SELECT COUNT.*information_schema.TABLES").WithArgs("test_trace_statistics", "test_call_statistics").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
	if ready {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.StatisticsReceiptColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("char(64)", "STORED GENERATED", ingest.StatisticsReceiptExpression))
		mock.ExpectQuery("SELECT COUNT.*information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(31))
		mock.ExpectQuery("SELECT COUNT.*information_schema.STATISTICS").WillReturnRows(sqlmock.NewRows([]string{"primary", "matching"}).AddRow(7, 7))
	}
}

func expectLifecycleSuspended(mock sqlmock.Sqlmock) {
	expectLifecycleBaseReady(mock, 7)
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, false)
	mock.ExpectQuery("SELECT generation FROM .*enabled=TRUE.*suspended=FALSE").WillReturnError(sql.ErrNoRows)
	expectLifecycleContentReady(mock)
	// Nil from the optional initializer is not proof that the schema exists.
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, false)
}

func TestProjectionLifecycleSuspendedMissingSchemaRemainsPending(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleSuspended(mock)
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.NoError(t, err)
	require.False(t, complete, "durable OFF with absent optional schema must remain pending, not stop recovery")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleSuspendedStartResumesAfterTransientDDLFailure(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleSuspended(mock)
	// The operator has resumed the base model. Exercise the real initializer's
	// lease/DDL error path, followed by a now-ready schema on the next retry.
	expectLifecycleBaseReady(mock, 8)
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, false)
	mock.ExpectQuery("SELECT generation FROM").WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(8))
	mock.ExpectExec("UPDATE .*SET migration_owner=\\?,migration_until=UNIX_TIMESTAMP\\(\\)\\+300 WHERE").WithArgs(sqlmock.AnyArg(), 1, int64(8)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*trace_statistics").WillReturnError(errors.New("temporary DDL unavailability"))
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*SET migration_owner='',migration_until=0 WHERE").WithArgs(sqlmock.AnyArg(), int64(8)).WillReturnResult(sqlmock.NewResult(0, 1))
	expectLifecycleSummariesReady(mock)
	expectLifecycleBaseReady(mock, 8)
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, true)
	expectLifecycleContentReady(mock)
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts int
	retryModelsForLifecycle(ctx, time.Millisecond, time.Millisecond, func(attemptCtx context.Context) (bool, error) {
		attempts++
		return initializeModelsForLifecycle(attemptCtx, db)
	})
	require.Equal(t, 3, attempts, "the OFF nil result must not terminate the owned recovery loop")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleCloseCancelsPendingWait(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &provider{db: db, maintenanceCancel: cancel}
	pending, done := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	go func() {
		defer close(done)
		retryModelsForLifecycle(ctx, time.Millisecond, time.Hour, func(context.Context) (bool, error) {
			attempts.Add(1)
			close(pending)
			return false, nil
		})
	}()
	select {
	case <-pending:
	case <-time.After(time.Second):
		t.Fatal("pending initialization did not run")
	}
	select {
	case <-done:
		t.Fatal("nil pending result stopped the worker before Close")
	case <-time.After(10 * time.Millisecond):
	}
	mock.ExpectClose()
	require.NoError(t, p.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the idle wait")
	}
	require.EqualValues(t, 1, attempts.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleUnsupportedEngineStopsWithoutOptionalSchema(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleBaseReady(mock, 7)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36-MySQL"))
	expectLifecycleContentReady(mock)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36-MySQL"))
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.NoError(t, err)
	require.True(t, complete)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleConfiguredOffStopsBeforeOptionalInitialization(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "false")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "version"}).AddRow(true, 7, 1))
	mock.ExpectExec("UPDATE .*SET generation=generation.*enabled=FALSE,suspended=TRUE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.NoError(t, err)
	require.True(t, complete)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleCloseCancelsActiveInitialization(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &provider{db: db, maintenanceCancel: cancel}
	started, done := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	go func() {
		defer close(done)
		retryModelsForLifecycle(ctx, time.Millisecond, time.Hour, func(attemptCtx context.Context) (bool, error) {
			attempts.Add(1)
			close(started)
			<-attemptCtx.Done()
			return false, attemptCtx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initialization did not start")
	}
	mock.ExpectClose()
	require.NoError(t, p.Close())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the active attempt")
	}
	require.EqualValues(t, 1, attempts.Load())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleStatisticsFailureStillChecksSummaryReadiness(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleBaseReady(mock, 7)
	failure := errors.New("optional statistics inspection unavailable")
	mock.ExpectQuery("SELECT VERSION").WillReturnError(failure)
	expectLifecycleSummariesReady(mock)
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.ErrorIs(t, err, failure)
	require.False(t, complete, "statistics recovery remains pending after independent summary readiness")
	require.NoError(t, mock.ExpectationsWereMet(), "an optional statistics failure must not skip summary initialization")
}

func TestProjectionStartupDefersOptionalBackfillToOwnedRetry(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	// Establish the mandatory generation/eligibility fence before returning.
	// No optional statistics or generated-column probes/DDL may block startup.
	expectLifecycleBaseReady(mock, 7)
	complete, err := initializeProjectionModelsForStartup(context.Background(), db, "test")
	require.NoError(t, err)
	require.False(t, complete, "the provider must schedule its existing owned retry, not declare optional migration complete")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionStartupBaseFailureRemainsPending(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	failure := errors.New("base control unavailable")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*projection_maintenance").WillReturnError(failure)
	complete, err := initializeProjectionModelsForStartup(context.Background(), db, "test")
	require.ErrorIs(t, err, failure)
	require.False(t, complete)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionLifecycleSuspendedMetricsUpgradeRemainsPending(t *testing.T) {
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	expectLifecycleBaseReady(mock, 7)
	expectLifecycleVersion(mock)
	expectLifecycleStatisticsReady(mock, true)
	columns := [][2]string{{ingest.TraceProjectionMetadataColumn, ingest.TraceProjectionMetadataExpression}, {ingest.TraceProjectionContentColumn, ingest.TraceProjectionContentExpression}, {ingest.TraceProjectionMetricsVersionColumn, ingest.TraceProjectionMetricsVersionExpression}, {ingest.TraceProjectionOperationPreviewsColumn, ingest.TraceProjectionOperationPreviewsExpression}}
	for _, column := range columns {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
	}
	for _, column := range []string{ingest.TraceProjectionMetricsColumn, ingest.TraceProjectionRowKeysColumn} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column).WillReturnError(sql.ErrNoRows)
	}
	mock.ExpectQuery("SELECT generation FROM .*enabled=TRUE.*suspended=FALSE").WillReturnError(sql.ErrNoRows)
	for _, column := range columns[:2] {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
	}
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionMetadataColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", ingest.TraceProjectionMetadataExpression))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionMetricsColumn).WillReturnError(sql.ErrNoRows)
	complete, err := initializeModelsForLifecycle(context.Background(), db)
	require.NoError(t, err)
	require.False(t, complete, "resume must retain a live worker for absent metrics helpers")
	require.NoError(t, mock.ExpectationsWereMet())
}
