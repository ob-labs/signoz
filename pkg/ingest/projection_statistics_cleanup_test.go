package ingest

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func expectOptionalStatisticsCleanupAbsent(mock sqlmock.Sqlmock, generation int64) {
	for _, suffix := range []string{"call_statistics", "trace_statistics"} {
		mock.ExpectBegin()
		expectProjectionCleanupFence(mock, sqlmock.AnyArg(), generation, false)
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `test_" + suffix + "` LIMIT 256")).WillReturnError(&mysql.MySQLError{Number: 1146})
		mock.ExpectRollback()
	}
}

// A missing optional cleanup would leave deleted Trace contributions forever.
// Hand-specified row counts prove that every contribution table is drained in
// short transactions and that an interrupted attempt can retry the remainder.
func TestProjectionResetDrainsOptionalStatisticsInBoundedChunks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, suffix := range []string{"trace_projections", "trace_sources", "trace_projection_coverage"} {
		expectProjectionCleanupChunk(mock, suffix, 3, 0, false)
	}
	expectProjectionCleanupChunk(mock, "call_statistics", 3, 256, false)
	expectProjectionCleanupChunk(mock, "call_statistics", 3, 11, false)
	expectProjectionCleanupChunk(mock, "trace_statistics", 3, 1, false)
	if err := resetProjectionDerived(context.Background(), db, "test", projectionCleanupLease{"owner", 3}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOptionalStatisticsCleanupAllowsMissingTableButNotDeniedDelete(t *testing.T) {
	for _, test := range []struct {
		name      string
		failure   error
		wantError bool
	}{
		{"old deployment", &mysql.MySQLError{Number: 1146, Message: "table does not exist"}, false},
		{"denied", &mysql.MySQLError{Number: 1142, Message: "DELETE denied"}, true},
		{"transport", errors.New("connection lost"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			expectProjectionCleanupFence(mock, "owner", 3, false)
			mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `test_call_statistics` LIMIT 256")).WillReturnError(test.failure)
			mock.ExpectRollback()
			count, err := deleteProjectionDerivedChunk(context.Background(), db, "test", "call_statistics", projectionCleanupLease{"owner", 3})
			if (err != nil) != test.wantError || count != 0 {
				t.Fatalf("count=%d err=%v", count, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOptionalStatisticsCleanupStillChecksGenerationBeforeDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT suspended FROM `test_projection_maintenance`").WithArgs("owner", int64(2)).WillReturnRows(sqlmock.NewRows([]string{"suspended"}))
	mock.ExpectRollback()
	if _, err := deleteProjectionDerivedChunk(context.Background(), db, "test", "trace_statistics", projectionCleanupLease{"owner", 2}); err == nil {
		t.Fatal("obsolete generation must not delete contributions")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
