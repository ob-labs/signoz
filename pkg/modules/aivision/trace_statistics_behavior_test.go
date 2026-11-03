package aivision

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"
)

func expectStatisticsReady(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("information_schema.TABLES").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"type", "extra", "expression"}).AddRow("char(64)", "STORED GENERATED", "JSON_UNQUOTE(JSON_EXTRACT(row_json,'$._statistics.receipt'))"))
	mock.ExpectQuery("information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(31))
	mock.ExpectQuery("information_schema.STATISTICS").WillReturnRows(sqlmock.NewRows([]string{"primary", "matching"}).AddRow(7, 7))
}

func TestStatisticsAggregateDeclinesIncompleteAndEmergencyOff(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "maintenance_off", true: "dirty_boundary_or_receipt_missing"}[enabled], func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, enabled, false)
			if enabled {
				mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
				expectStatisticsReady(mock)
				mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(1, 2, 1, 0, 0, 0, 0, 0, 0, nil))
			}
			mock.ExpectRollback()
			value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model")
			require.NoError(t, e)
			require.Equal(t, false, value.(map[string]any)["supported"])
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestStatisticsPublicationFailureRollsBackCanonicalAndContributions(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := statisticsFixture()
	p.UserID = "owner"
	p.AgentProduct = "product"
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	expectPublicationSource(mock, p, 1)
	mock.ExpectExec("INSERT INTO .*trace_projections").WillReturnResult(sqlmock.NewResult(0, 1))
	expectStatisticsReady(mock)
	mock.ExpectExec("DELETE FROM .*call_statistics").WillReturnResult(sqlmock.NewResult(0, 7))
	mock.ExpectExec("DELETE FROM .*trace_statistics").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO .*trace_statistics").WillReturnResult(sqlmock.NewResult(0, 1))
	failure := errors.New("call contribution write failed")
	mock.ExpectExec("INSERT INTO .*call_statistics").WillReturnError(failure)
	mock.ExpectRollback()
	result, e := store.publishTraceProjection(context.Background(), "org", p)
	require.ErrorIs(t, e, failure)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStatisticsRequestRejectsRepeatedAndUnsafeSQLDimensions(t *testing.T) {
	h := &handler{cfg: config{MaxQueryRange: 1000000000000}}
	for _, suffix := range []string{"&bucket=minute", "&group_by=provider", "&raw=true", "&filters=[]"} {
		request := httptest.NewRequest("GET", "/?from=100&to=200&space_id=s&bucket=hour&group_by=model&projection_context="+strings.Repeat("a", 64)+suffix, nil)
		_, _, _, e := h.statisticsParams(request)
		require.Error(t, e)
	}
	store := &oceanBaseStore{}
	_, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model;DROP TABLE")
	require.Error(t, e)
}

func TestStatisticsTokenSumCannotOverflowBeforeFallback(t *testing.T) {
	_, ok := statisticsToken([]byte(strings.Repeat("9", 59)))
	require.True(t, ok)
	_, ok = statisticsToken([]byte(strings.Repeat("9", 60)))
	require.False(t, ok)
	row := map[string]any{"totalSum": strings.Repeat("9", 65)}
	require.NoError(t, normalizeStatisticsRow(row))
}

func TestStatisticsIneligibleReceiptPersistsWithoutPoisoningBase(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	p := statisticsFixture()
	p.UserID = "owner"
	p.AgentProduct = "product"
	p.Row = json.RawMessage(strings.Replace(string(p.Row), `"model":"m"`, `"model":123`, 1))
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	expectPublicationSource(mock, p, 1)
	mock.ExpectExec("INSERT INTO .*trace_projections").WillReturnResult(sqlmock.NewResult(0, 1))
	expectStatisticsReady(mock)
	mock.ExpectExec("DELETE FROM .*call_statistics").WithArgs("org", p.SpaceID, p.TraceID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM .*trace_statistics").WithArgs("org", p.SpaceID, p.TraceID).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO .*trace_statistics").WithArgs("org", p.SpaceID, p.TraceID, p.SourceRevision, p.ProjectionContext, strings.Repeat("a", 64), false, int64(2), int64(1), int64(1), 2.0, 0).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*trace_sources.*SET dirty=FALSE").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	result, e := store.publishTraceProjection(context.Background(), "org", p)
	require.NoError(t, e)
	require.Equal(t, true, result.(map[string]any)["published"])
	require.NoError(t, mock.ExpectationsWereMet())
}
