package ingest

import (
	"context"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"regexp"
	"strings"
	"testing"
)

func TestTraceSourceKeysCoalesceAndKeepTenantsSeparate(t *testing.T) {
	rows := [][]any{{"org", "space", 1, "a"}, {"org", "space", 2, "a"}, {"org", "other", 3, "a"}, {"other", "space", 4, "a"}}
	keys := traceSourceKeys(rows)
	if len(keys) != 3 {
		t.Fatalf("got %v; want one revision per affected trace and tenant", keys)
	}
	if keys[0].OrgID != "org" || keys[0].SpaceID != "other" {
		t.Fatalf("lock keys must be ordered: %v", keys)
	}
}

func TestSameSpanStartCorrectionUpdatesIndexedTimestamp(t *testing.T) {
	if !strings.Contains(spanUpsert, "timestamp=VALUES(timestamp)") {
		t.Fatal("same-span correction leaves indexed timestamp stale")
	}
}

func TestTraceRevisionRollsBackWithIngest(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ep := &endpoint{db: db, cfg: Config{TablePrefix: "test"}, tracesTable: "`test_traces`"}
	rows := [][]any{{"org", "space", 1, "abc"}, {"org", "space", 2, "abc"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance` WHERE id=1 FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 1, 1))
	mock.ExpectExec("INSERT INTO `test_trace_sources`").WithArgs("org", "space", "abc").WillReturnResult(sqlmock.NewResult(0, 1))
	prepared := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `test_traces` (x) VALUES (?)"))
	prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `test_trace_sources`").WithArgs("org", "space", "abc").WillReturnError(fmt.Errorf("revision write failed"))
	mock.ExpectRollback()
	if err := ep.insertRows(context.Background(), ep.tracesTable, " (x) VALUES (?)", "", rows); err == nil {
		t.Fatal("ingest must not commit spans if revision write fails")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestTraceIngestCoalescesRevisionWithinTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ep := &endpoint{db: db, cfg: Config{TablePrefix: "test"}, tracesTable: "`test_traces`"}
	rows := [][]any{{"org", "space", 1, "abc"}, {"org", "space", 2, "abc"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance` WHERE id=1 FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 1, 1))
	mock.ExpectExec("INSERT INTO `test_trace_sources`").WithArgs("org", "space", "abc").WillReturnResult(sqlmock.NewResult(0, 1))
	prepared := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `test_traces` (x) VALUES (?)"))
	prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	prepared.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `test_trace_sources`").WithArgs("org", "space", "abc").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := ep.insertRows(context.Background(), ep.tracesTable, " (x) VALUES (?)", "", rows); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTrackingDisabledRawIngestRevokesSharedEligibility(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ep := &endpoint{db: db, cfg: Config{TablePrefix: "test", ProjectionMaintenanceDisabled: true}, tracesTable: "`test_traces`"}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM `test_projection_maintenance` WHERE id=1 FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 1, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `test_projection_maintenance` SET generation=generation+IF(enabled,1,0),enabled=FALSE,suspended=TRUE,rebuild_required=TRUE")).WillReturnResult(sqlmock.NewResult(0, 1))
	stmt := mock.ExpectPrepare(regexp.QuoteMeta("INSERT INTO `test_traces` (x) VALUES (?)"))
	stmt.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := ep.insertRows(context.Background(), ep.tracesTable, " (x) VALUES (?)", "", [][]any{{"org", "space", 1, "abc"}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
