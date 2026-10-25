package aivision

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// Observe the actual BeginTx options, which sqlmock does not expose.
type maintenanceIsolationDriver struct{ options chan driver.TxOptions }
type maintenanceIsolationConn struct{ options chan driver.TxOptions }
type maintenanceIsolationTx struct{}

func (d maintenanceIsolationDriver) Open(string) (driver.Conn, error) {
	return maintenanceIsolationConn{d.options}, nil
}
func (c maintenanceIsolationConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c maintenanceIsolationConn) Close() error { return nil }
func (c maintenanceIsolationConn) Begin() (driver.Tx, error) {
	return nil, errors.New("expected BeginTx")
}
func (c maintenanceIsolationConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.options <- options
	return maintenanceIsolationTx{}, nil
}
func (maintenanceIsolationTx) Commit() error   { return nil }
func (maintenanceIsolationTx) Rollback() error { return nil }

var maintenanceIsolationSequence atomic.Uint64

func TestReadModelKeepsReadSnapshotsRepeatableAndWritesCurrent(t *testing.T) {
	options := make(chan driver.TxOptions, 2)
	name := fmt.Sprintf("maintenance-isolation-%d", maintenanceIsolationSequence.Add(1))
	sql.Register(name, maintenanceIsolationDriver{options})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db}
	for _, readOnly := range []bool{true, false} {
		tx, err := s.beginReadModel(context.Background(), readOnly)
		require.NoError(t, err)
		got := <-options
		want := sql.LevelReadCommitted
		if readOnly {
			want = sql.LevelRepeatableRead
		}
		require.Equal(t, driver.IsolationLevel(want), got.Isolation)
		require.Equal(t, readOnly, got.ReadOnly)
		require.NoError(t, tx.Rollback())
	}
}

func maintenanceCoverageRows(after string, complete bool) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"after_trace_id", "complete"}).AddRow(after, complete)
}

func expectMaintenanceBackfillProbe(mock sqlmock.Sqlmock, after string, complete bool) {
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("^SELECT after_trace_id,complete FROM .* WHERE org_id=\\? AND space_id=\\?$").WithArgs("org", "space").WillReturnRows(maintenanceCoverageRows(after, complete))
	if complete {
		mock.ExpectCommit()
	} else {
		mock.ExpectRollback()
	}
}

func maintenanceAggregateRows(first, count int) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"trace_id", "min_timestamp", "max_timestamp", "user_id", "agent_product", "scope_valid"})
	for i := first; i < first+count; i++ {
		rows.AddRow(fmt.Sprintf("%032x", i), int64(100+i), int64(200+i), "owner", "product", true)
	}
	return rows
}

func expectMaintenanceChunkStart(mock sqlmock.Sqlmock, after string, limit, first, count int) {
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	mock.ExpectExec("INSERT IGNORE INTO .*trace_projection_coverage").WithArgs("org", "space").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT after_trace_id,complete FROM .*FOR UPDATE").WithArgs("org", "space").WillReturnRows(maintenanceCoverageRows(after, false))
	mock.ExpectQuery("SELECT trace_id,MIN\\(timestamp\\)").WithArgs("org", "space", after, limit).WillReturnRows(maintenanceAggregateRows(first, count))
}

func expectMaintenanceSources(mock sqlmock.Sqlmock, first, count int, delay time.Duration, failure error) {
	values := make([]driver.Value, 0, count*8)
	placeholders := make([]string, 0, count)
	for index := first; index < first+count; index++ {
		placeholders = append(placeholders, "(?,?,?,1,?,?,?,?,?,TRUE)")
		values = append(values, "org", "space", fmt.Sprintf("%032x", index), int64(100+index), int64(200+index), "owner", "product", true)
	}
	query := "INSERT INTO `test_trace_sources` (org_id,space_id,trace_id,source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid,dirty) VALUES " + strings.Join(placeholders, ",") + " ON DUPLICATE KEY UPDATE source_revision=source_revision+1,dirty=TRUE,min_timestamp=VALUES(min_timestamp),max_timestamp=VALUES(max_timestamp),user_id=VALUES(user_id),agent_product=VALUES(agent_product),scope_valid=VALUES(scope_valid)"
	expect := mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(values...)
	if delay > 0 {
		expect.WillDelayFor(delay)
	}
	if failure != nil {
		expect.WillReturnError(failure)
	} else {
		// MySQL may report two affected rows per updated source. Enrollment
		// counts input identities, not engine-specific affected-row totals.
		expect.WillReturnResult(sqlmock.NewResult(0, int64(count*2)))
	}
}

func expectMaintenanceChunkCommit(mock sqlmock.Sqlmock, last int, complete bool) {
	mock.ExpectExec("UPDATE .*trace_projection_coverage.* SET after_trace_id=\\?,complete=\\?").WithArgs(fmt.Sprintf("%032x", last), complete, "org", "space").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func TestBackfillCompletedCoverageDoesNotTakeMaintenanceWriteLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectMaintenanceBackfillProbe(mock, "abc", true)
	result, err := s.backfillTraceSources(context.Background(), "org", "space", 500)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"supported": true, "after_trace_id": "abc", "complete": true, "enrolled": 0}, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBackfillCommitsAtMost25AndRereadsSharedCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectMaintenanceBackfillProbe(mock, "", false)
	expectMaintenanceChunkStart(mock, "", 25, 1, 25)
	expectMaintenanceSources(mock, 1, 25, 0, nil)
	expectMaintenanceChunkCommit(mock, 25, false)
	// Another maintainer advanced the persisted cursor between transactions.
	expectMaintenanceChunkStart(mock, fmt.Sprintf("%032x", 28), 1, 29, 1)
	expectMaintenanceSources(mock, 29, 1, 0, nil)
	expectMaintenanceChunkCommit(mock, 29, false)
	result, err := s.backfillTraceSources(context.Background(), "org", "space", 26)
	require.NoError(t, err)
	response := result.(map[string]any)
	require.Equal(t, 26, response["enrolled"])
	require.Equal(t, fmt.Sprintf("%032x", 29), response["after_trace_id"])
	require.Equal(t, false, response["complete"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBackfillChunkFailureDoesNotAdvanceCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectMaintenanceBackfillProbe(mock, "", false)
	expectMaintenanceChunkStart(mock, "", 25, 1, 2)
	failure := errors.New("source batch failed")
	expectMaintenanceSources(mock, 1, 2, 0, failure)
	mock.ExpectRollback()
	_, err = s.backfillTraceSources(context.Background(), "org", "space", 500)
	require.ErrorIs(t, err, failure)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBackfillStopsOnMaintenanceTransitionBetweenChunks(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			expectMaintenanceBackfillProbe(mock, "", false)
			expectMaintenanceChunkStart(mock, "", 25, 1, 25)
			expectMaintenanceSources(mock, 1, 25, 0, nil)
			expectMaintenanceChunkCommit(mock, 25, false)
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT enabled,generation,schema_version FROM.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(enabled, 2, 1))
			mock.ExpectRollback()
			result, err := s.backfillTraceSources(context.Background(), "org", "space", 500)
			require.NoError(t, err)
			reason := "projection_maintenance_unavailable"
			if enabled {
				reason = "maintenance_generation_changed"
			}
			require.Equal(t, readModelUnsupported(reason), result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestBackfillYieldsCommittedProgressAfterSoftWorkBudget(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectMaintenanceBackfillProbe(mock, "", false)
	expectMaintenanceChunkStart(mock, "", 25, 1, 25)
	expectMaintenanceSources(mock, 1, 25, 2100*time.Millisecond, nil)
	expectMaintenanceChunkCommit(mock, 25, false)
	result, err := s.backfillTraceSources(context.Background(), "org", "space", 500)
	require.NoError(t, err)
	require.Equal(t, 25, result.(map[string]any)["enrolled"])
	require.Equal(t, false, result.(map[string]any)["complete"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBackfillBatchValidatesTheWholePageBeforeWriting(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectMaintenanceBackfillProbe(mock, "", false)
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	mock.ExpectExec("INSERT IGNORE INTO .*trace_projection_coverage").WithArgs("org", "space").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT after_trace_id,complete FROM .*FOR UPDATE").WithArgs("org", "space").WillReturnRows(maintenanceCoverageRows("", false))
	rows := maintenanceAggregateRows(1, 1).AddRow(fmt.Sprintf("%032x", 2), int64(200), int64(100), "owner", "product", true)
	mock.ExpectQuery("SELECT trace_id,MIN\\(timestamp\\)").WithArgs("org", "space", "", 25).WillReturnRows(rows)
	// The invalid second row must not cause any source write or cursor change.
	mock.ExpectRollback()
	_, err = s.backfillTraceSources(context.Background(), "org", "space", 500)
	require.ErrorContains(t, err, "invalid historical trace summary")
	require.NoError(t, mock.ExpectationsWereMet())
}
