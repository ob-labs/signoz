package aivision

import (
	"context"
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func statisticsGateRows(values ...driver.Value) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"candidateTraceCount", "candidateSpanCount", "incompleteCount", "traceCount", "spanCount", "generationCount", "toolCount", "errorCount", "latencyCount", "latencySum"}).AddRow(values...)
}

func statisticsPartitionFixture(kind any, dimension any, bucket any, gKind, gDimension, gBucket int, calls, operations int64) map[string]any {
	row := map[string]any{"kind": kind, "dimension": dimension, "bucket_ns": bucket, "grouping_kind": gKind, "grouping_dimension": gDimension, "grouping_bucket": gBucket, "callCount": calls, "operationCount": operations, "ambiguousCount": int64(0), "costCurrencyInvalid": calls}
	for _, field := range []string{"input", "output", "total", "cacheReadInput", "cacheWriteInput", "reasoning", "costTotal", "costInput", "costOutput"} {
		row[field+"Known"] = int64(0)
		row[field+"Sum"] = nil
	}
	return row
}

func TestStatisticsPartitionsPreserveExactNullableDimensions(t *testing.T) {
	rows := []map[string]any{statisticsPartitionFixture(nil, nil, nil, 1, 1, 1, 5, 5), statisticsPartitionFixture("GENERATION", nil, nil, 0, 1, 1, 5, 5), statisticsPartitionFixture(nil, nil, "0", 1, 1, 0, 5, 5), statisticsPartitionFixture("GENERATION", nil, "0", 0, 1, 0, 5, 5)}
	for _, dimension := range []any{nil, "", "Foo", "foo", "Foo "} {
		rows = append(rows, statisticsPartitionFixture(nil, dimension, nil, 1, 0, 1, 1, 1))
	}
	result, e := partitionStatisticsRows(rows)
	require.NoError(t, e)
	groups := result["groups"].([]map[string]any)
	require.Len(t, groups, 5)
	values := []any{}
	for _, row := range groups {
		values = append(values, row["value"])
		require.EqualValues(t, 1, row["callCount"])
	}
	require.Equal(t, []any{nil, "", "Foo", "foo", "Foo "}, values)
}

func TestStatisticsPartitionsHandleToolOnlyAndNoCalls(t *testing.T) {
	for _, toolOnly := range []bool{false, true} {
		rows := []map[string]any{}
		if toolOnly {
			rows = []map[string]any{statisticsPartitionFixture(nil, nil, nil, 1, 1, 1, 0, 1), statisticsPartitionFixture("TOOL", nil, nil, 0, 1, 1, 0, 1), statisticsPartitionFixture(nil, nil, nil, 1, 0, 1, 0, 1), statisticsPartitionFixture(nil, nil, "0", 1, 1, 0, 0, 1), statisticsPartitionFixture("TOOL", nil, "0", 0, 1, 0, 0, 1)}
		}
		result, e := partitionStatisticsRows(rows)
		require.NoError(t, e)
		require.EqualValues(t, 0, result["usage"].(map[string]any)["callCount"])
		require.Nil(t, result["usage"].(map[string]any)["totalSum"])
		for _, key := range []string{"usageByType", "groups", "timeseries"} {
			require.Empty(t, result[key])
		}
		operations := result["operations"].([]map[string]any)
		if toolOnly {
			require.Equal(t, []map[string]any{{"type": "TOOL", "start_ns": "0", "count": int64(1)}}, operations)
		} else {
			require.Empty(t, operations)
		}
	}
}

func TestStatisticsServingDeclinesOlderOBWithRetainedTables(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.3.5.3"))
	mock.ExpectRollback()
	value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model")
	require.NoError(t, e)
	require.Equal(t, map[string]any{"supported": false, "reason": "statistics_engine_unavailable"}, value)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStatisticsCombinedGateKeepsAgentSelectionOutOfCompleteness(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "test"}}
	query, args, e := store.statisticsGateSummarySQL("org", windowTestParams())
	require.NoError(t, e)
	before, after, found := strings.Cut(query, " LEFT JOIN (")
	require.True(t, found)
	require.Contains(t, before, "BINARY p.agent_uuid=BINARY ?")
	require.NotContains(t, after, "p.agent_uuid")
	joined, predicate, found := strings.Cut(after, ") ON (")
	require.True(t, found)
	require.NotContains(t, joined, "r.span_count")
	require.Contains(t, predicate, "t.span_count=r.span_count")
	require.NotContains(t, query, "row_json")
	require.Equal(t, "agent", args[0])
}

func TestStatisticsCombinedEmptyWindowPreservesUnknownUsage(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
	expectStatisticsReady(mock)
	mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(0, 0, 0, 0, 0, 0, 0, 0, 0, nil))
	mock.ExpectQuery("SELECT GROUPING.*GROUP BY GROUPING SETS").WillReturnRows(sqlmock.NewRows([]string{"grouping_kind", "grouping_dimension", "grouping_bucket"}))
	mock.ExpectCommit()
	value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model")
	require.NoError(t, e)
	result := value.(map[string]any)
	require.Equal(t, true, result["complete"])
	require.EqualValues(t, 0, result["summary"].(map[string]any)["traceCount"])
	require.Nil(t, result["usage"].(map[string]any)["totalSum"])
	require.EqualValues(t, 0, result["usage"].(map[string]any)["totalKnown"])
	require.Empty(t, result["operations"])
	require.Empty(t, result["latencies"])
	require.NoError(t, mock.ExpectationsWereMet())
}
