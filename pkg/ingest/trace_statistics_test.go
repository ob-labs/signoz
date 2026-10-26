package ingest

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatisticsOptionalLeaseNeverInvalidatesBaseProjection(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT generation FROM .*projection_maintenance").WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(7))
	mock.ExpectExec("UPDATE .*projection_maintenance.*SET migration_owner=\\?,migration_until=UNIX_TIMESTAMP\\(\\)\\+300 WHERE").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(7)).WillReturnResult(sqlmock.NewResult(0, 0))
	require.Error(t, InitializeTraceStatistics(context.Background(), db, "test"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStatisticsReceiptRequiresDatabaseDerivedColumn(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"type", "extra", "expression"}).AddRow("char(64)", "", ""))
	ready, e := TraceStatisticsSchemaReady(context.Background(), db, "test")
	require.NoError(t, e)
	require.False(t, ready)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStatisticsInitializerLeavesDurableOffLatchIdle(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT generation FROM").WillReturnError(sql.ErrNoRows)
	require.NoError(t, InitializeTraceStatistics(context.Background(), db, "test"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStatisticsInitializerUsesEngineRowDDL(t *testing.T) {
	for _, test := range []struct {
		name, version, suffix string
	}{
		{"oceanbase_explicit_row", "5.7.25-OceanBase_CE-v4.4.2.1", ` WITH COLUMN GROUP\(all columns\)`},
		{"seekdb_default_row", "5.7.25-OceanBase seekdb-v1.4.0.0", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(test.version))
			mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
			mock.ExpectQuery("SELECT generation FROM .*projection_maintenance").WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(7))
			mock.ExpectExec("UPDATE .*projection_maintenance.*SET migration_owner=\\?,migration_until=UNIX_TIMESTAMP\\(\\)\\+300 WHERE").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"timeout"}).AddRow(10000000))
			mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
			// Stop at the real DDL boundary. The selected statement must have the
			// engine's exact suffix; the injected DB error must still propagate.
			stop := errors.New("DDL execution stopped by test")
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*trace_statistics.*COLLATE=utf8mb4_bin" + test.suffix + "$").WillReturnError(stop)
			mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec("UPDATE .*projection_maintenance.*SET migration_owner='',migration_until=0 WHERE").WithArgs(sqlmock.AnyArg(), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
			e = InitializeTraceStatistics(context.Background(), db, "test")
			require.ErrorIs(t, e, stop)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
