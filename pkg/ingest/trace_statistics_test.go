package ingest

import (
	"context"
	"database/sql"
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
