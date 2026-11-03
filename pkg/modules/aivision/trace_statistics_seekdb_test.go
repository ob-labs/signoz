package aivision

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const seekdbStatisticsTestVersion = "5.7.25-OceanBase seekdb-v1.4.0.0"

func seekdbStatisticsRows(rows ...map[string]any) *sqlmock.Rows {
	columns := []string{"grouping_kind", "grouping_dimension", "grouping_bucket", "kind", "dimension", "bucket_ns", "operationCount", "callCount", "ambiguousCount", "costCurrencyInvalid"}
	for _, field := range []string{"input", "output", "total", "cacheReadInput", "cacheWriteInput", "reasoning", "costTotal", "costInput", "costOutput"} {
		columns = append(columns, field+"Sum", field+"Known")
	}
	result := sqlmock.NewRows(columns)
	for _, row := range rows {
		values := make([]driver.Value, len(columns))
		for i, column := range columns {
			values[i] = row[column]
		}
		result.AddRow(values...)
	}
	return result
}

// Reusing the OB GROUPING SETS branch on seekdb must fail this boundary test;
// the real SQL/NULL behavior is additionally checked by the isolated SQL lab.
func TestSeekdbStatisticsUsesCompatibleGroupingAndPreservesUnknowns(t *testing.T) {
	for _, toolOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "tool_only"}[toolOnly], func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(seekdbStatisticsTestVersion))
			expectStatisticsReady(mock)
			rows := []map[string]any{}
			if toolOnly {
				mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(1, 1, 0, 1, 1, 0, 1, 0, 0, nil))
				rows = []map[string]any{statisticsPartitionFixture(nil, nil, nil, 1, 1, 1, 0, 1), statisticsPartitionFixture("TOOL", nil, nil, 0, 1, 1, 0, 1), statisticsPartitionFixture(nil, nil, nil, 1, 0, 1, 0, 1), statisticsPartitionFixture(nil, nil, "0", 1, 1, 0, 0, 1), statisticsPartitionFixture("TOOL", nil, "0", 0, 1, 0, 0, 1)}
			} else {
				mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(0, 0, 0, 0, 0, 0, 0, 0, 0, nil))
			}
			mock.ExpectQuery("SELECT .*CROSS JOIN.*SELECT 0 AS axis.*GROUP BY").WillReturnRows(seekdbStatisticsRows(rows...))
			mock.ExpectCommit()
			value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model")
			require.NoError(t, e)
			result := value.(map[string]any)
			require.Equal(t, true, result["complete"])
			require.Equal(t, "native-trace-metrics-aggregate-v1", result["contract"])
			require.EqualValues(t, 0, result["usage"].(map[string]any)["totalKnown"])
			require.Nil(t, result["usage"].(map[string]any)["totalSum"])
			require.Empty(t, result["groups"])
			if toolOnly {
				require.Equal(t, []map[string]any{{"type": "TOOL", "start_ns": "0", "count": int64(1)}}, result["operations"])
			} else {
				require.Empty(t, result["operations"])
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSeekdbOperationPagePreservesGenerationWithoutAggregationSyntax(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version FROM").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "version"}).AddRow(true, 7, 1))
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(seekdbStatisticsTestVersion))
	expectStatisticsReady(mock)
	mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(1, 2, 0, 1, 2, 1, 1, 0, 0, nil))
	mock.ExpectQuery("SELECT .*ORDER BY c.start_ns DESC,BINARY t.trace_id DESC,BINARY c.observation_id DESC LIMIT \\?").WillReturnRows(operationRows().AddRow("a", "f", "GENERATION", "150000001", 4, 2).AddRow("a", "e", "TOOL", "150000000", 4, 2))
	mock.ExpectCommit()
	value, e := store.traceOperationPage(context.Background(), "org", windowTestParams(), 1)
	require.NoError(t, e)
	result := value.(map[string]any)
	require.Equal(t, true, result["has_more"])
	require.EqualValues(t, 7, result["meta"].(map[string]any)["maintenance_generation"])
	require.Equal(t, []map[string]any{{"trace_id": "a", "observation_id": "f", "type": "GENERATION", "start_ns": "150000001", "source_revision": int64(4), "span_count": int64(2)}}, result["data"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSeekdbStatisticsPreservesBinaryGroupsAndExactAccumulators(t *testing.T) {
	var emitted string
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		if strings.Contains(actual, "CROSS JOIN") {
			emitted = actual
		}
		return sqlmock.QueryMatcherRegexp.Match(expected, actual)
	})
	db, mock, e := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(seekdbStatisticsTestVersion))
	expectStatisticsReady(mock)
	mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(1, 5, 0, 1, 5, 5, 0, 0, 0, nil))
	rows := []map[string]any{statisticsPartitionFixture(nil, nil, nil, 1, 1, 1, 5, 5), statisticsPartitionFixture("GENERATION", nil, nil, 0, 1, 1, 5, 5), statisticsPartitionFixture(nil, nil, "0", 1, 1, 0, 5, 5), statisticsPartitionFixture("GENERATION", nil, "0", 0, 1, 0, 5, 5)}
	for _, row := range rows {
		row["totalSum"], row["totalKnown"] = "50000000000000000000000000000000000000000", int64(5)
		row["costTotalSum"], row["costTotalKnown"] = "0.000000000000000005", int64(5)
		row["costCurrencyInvalid"] = int64(0)
	}
	for _, dimension := range []any{nil, "", "Foo", "foo", "Foo "} {
		row := statisticsPartitionFixture(nil, dimension, nil, 1, 0, 1, 1, 1)
		row["totalSum"], row["totalKnown"] = "10000000000000000000000000000000000000000", int64(1)
		row["costTotalSum"], row["costTotalKnown"] = "0.000000000000000001", int64(1)
		row["costCurrencyInvalid"] = int64(0)
		rows = append(rows, row)
	}
	mock.ExpectQuery("SELECT .*CROSS JOIN.*GROUP BY").WillReturnRows(seekdbStatisticsRows(rows...))
	mock.ExpectCommit()
	value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "minute", "provider")
	require.NoError(t, e)
	result := value.(map[string]any)
	usage := result["usage"].(map[string]any)
	require.Equal(t, "50000000000000000000000000000000000000000", usage["totalSum"])
	require.Equal(t, "0.000000000000000005", usage["costTotalSum"])
	require.Nil(t, usage["inputSum"])
	require.EqualValues(t, 0, usage["inputKnown"])
	groups := result["groups"].([]map[string]any)
	values := []any{}
	for _, row := range groups {
		values = append(values, row["value"])
		require.Equal(t, "10000000000000000000000000000000000000000", row["totalSum"])
	}
	require.Equal(t, []any{nil, "", "Foo", "foo", "Foo "}, values)
	// These assertions concern the emitted SQL boundary, not an implementation
	// source string. Real seekdb execution validates the expression semantics.
	require.Contains(t, emitted, "THEN BINARY c.provider ELSE NULL END AS dimension")
	require.Contains(t, emitted, "CASE WHEN a.axis=2 THEN c.dimension ELSE NULL END AS dimension")
	require.Contains(t, emitted, "GROUP BY c.axis,c.grouped_kind,c.dimension,c.bucket_ns")
	require.Equal(t, 1, strings.Count(emitted, " AS span_count FROM "))
	require.NotContains(t, emitted, "GROUPING(")
	require.NotContains(t, emitted, "GROUPING SETS")
	require.NotContains(t, emitted, "row_json")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSeekdbStatisticsRejectsIncompleteUnselectedAgent(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(seekdbStatisticsTestVersion))
	expectStatisticsReady(mock)
	mock.ExpectQuery("SELECT COUNT.*candidateTraceCount").WillReturnRows(statisticsGateRows(1, 5, 1, 0, 0, 0, 0, 0, 0, nil))
	mock.ExpectRollback()
	value, e := store.traceMetricsAggregate(context.Background(), "org", windowTestParams(), "hour", "model")
	require.NoError(t, e)
	require.Equal(t, map[string]any{"supported": false, "reason": "statistics_window_incomplete"}, value)
	require.NoError(t, mock.ExpectationsWereMet())
}
