package ingest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func contentColumnRows(extra, expr string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow("tinyint(4)", extra, expr)
}

func TestTraceContentColumnCapabilityFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, extra, expr string
		ready             bool
	}{
		{"source", "STORED GENERATED", TraceContentCompleteExpression, true},
		{"engine", "STORED GENERATED", traceContentCompleteStoredExpression, true},
		{"ordinary", "", TraceContentCompleteExpression, false},
		{"virtual", "VIRTUAL GENERATED", TraceContentCompleteExpression, false},
		{"wrong", "STORED GENERATED", "1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", TraceContentCompleteColumn).WillReturnRows(contentColumnRows(tc.extra, tc.expr))
			ready, err := TraceContentColumnReady(context.Background(), db, "test")
			require.NoError(t, err)
			require.Equal(t, tc.ready, ready)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTraceContentColumnMissingIsCompatibleButInspectionFailureIsNot(t *testing.T) {
	for _, failure := range []error{sql.ErrNoRows, errors.New("metadata unavailable")} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnError(failure)
		ready, err := TraceContentColumnReady(context.Background(), db, "test")
		require.False(t, ready)
		if errors.Is(failure, sql.ErrNoRows) {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, failure)
		}
		require.NoError(t, mock.ExpectationsWereMet())
		db.Close()
	}
}

func TestTraceContentMigrationIsAdditiveAndKeepsProjectionEligibility(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	probe := "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION"
	mock.ExpectQuery(probe).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT generation FROM .*projection_maintenance.*enabled=TRUE.*suspended=FALSE.*rebuild_required=FALSE").WithArgs(ProjectionSchemaVersion).WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(7))
	mock.ExpectExec("UPDATE .*projection_maintenance.*SET migration_owner=\\?,migration_until=UNIX_TIMESTAMP\\(\\)\\+300 WHERE").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(probe).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE `test_trace_projections` ADD COLUMN " + TraceContentCompleteColumn + " TINYINT GENERATED ALWAYS AS (" + TraceContentCompleteExpression + ") STORED")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(probe).WillReturnRows(contentColumnRows("STORED GENERATED", TraceContentCompleteExpression))
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*projection_maintenance.*SET migration_owner='',migration_until=0 WHERE").WithArgs(sqlmock.AnyArg(), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, InitializeTraceContentColumn(context.Background(), db, "test"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTraceContentMigrationDoesNotReplaceConflictingColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(contentColumnRows("", "1"))
	require.Error(t, InitializeTraceContentColumn(context.Background(), db, "test"))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTraceContentMigrationLeavesDurableOffUntouched(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT generation FROM").WillReturnError(sql.ErrNoRows)
	require.NoError(t, InitializeTraceContentColumn(context.Background(), db, "test"))
	require.NoError(t, mock.ExpectationsWereMet())
}

// Never runs without an explicit isolated SQL DSN. Its unique two-table prefix
// does not touch retained benchmark tables or application telemetry.
func TestOceanBaseTraceContentColumnAdditiveSemantics(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for isolated real SQL validation")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("content_%x", time.Now().UnixNano())
	for _, suffix := range []string{"trace_projections", "projection_maintenance"} {
		suffix := suffix
		defer func() { _, e := db.Exec("DROP TABLE IF EXISTS " + tableName(prefix, suffix)); require.NoError(t, e) }()
	}
	_, err = db.ExecContext(ctx, "CREATE TABLE "+tableName(prefix, "trace_projections")+" (id INT PRIMARY KEY,row_json LONGTEXT NOT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	require.NoError(t, err)
	require.NoError(t, ensureProjectionMaintenanceControl(ctx, db, prefix))
	_, err = db.ExecContext(ctx, "UPDATE "+tableName(prefix, "projection_maintenance")+" SET enabled=TRUE,suspended=FALSE,rebuild_required=FALSE,schema_version=1,generation=7 WHERE id=1")
	require.NoError(t, err)
	documents := []string{`{}`, `{"_contentComplete":false}`, `{"_contentComplete":"false"}`, `{"_contentComplete":true}`, `{"_contentComplete":"False"}`, `{"_contentComplete":"false "}`, `{"_contentComplete":null}`}
	for i, body := range documents {
		_, err = db.ExecContext(ctx, "INSERT INTO "+tableName(prefix, "trace_projections")+" (id,row_json) VALUES (?,?)", i, body)
		require.NoError(t, err)
	}
	require.NoError(t, InitializeTraceContentColumn(ctx, db, prefix))
	ready, err := TraceContentColumnReady(ctx, db, prefix)
	require.NoError(t, err)
	require.True(t, ready)
	legacy := "CASE WHEN BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(row_json,'$._contentComplete')),'true')=BINARY 'false' THEN 0 ELSE 1 END"
	rows, err := db.QueryContext(ctx, "SELECT id,"+TraceContentCompleteColumn+","+legacy+" FROM "+tableName(prefix, "trace_projections")+" ORDER BY id")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var id, typed, old int
		require.NoError(t, rows.Scan(&id, &typed, &old))
		require.Equal(t, old, typed)
		if id == 1 || id == 2 {
			require.Equal(t, 0, typed)
		} else {
			require.Equal(t, 1, typed)
		}
		count++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, len(documents), count)
	_, err = db.ExecContext(ctx, "UPDATE "+tableName(prefix, "trace_projections")+" SET row_json=? WHERE id=0", `{"_contentComplete":false}`)
	require.NoError(t, err)
	var flag int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT "+TraceContentCompleteColumn+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=0").Scan(&flag))
	require.Zero(t, flag)
	require.NoError(t, InitializeTraceContentColumn(ctx, db, prefix))
	var enabled bool
	var generation int64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT enabled,generation FROM "+tableName(prefix, "projection_maintenance")+" WHERE id=1").Scan(&enabled, &generation))
	require.True(t, enabled)
	require.EqualValues(t, 7, generation)
}
