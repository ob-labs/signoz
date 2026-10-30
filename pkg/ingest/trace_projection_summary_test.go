package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func projectionSummaryColumnRows(kind, extra, expression string) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow(kind, extra, expression)
}

func TestProjectionSummaryCapabilityRejectsUnstoredOrChangedExpressions(t *testing.T) {
	for _, tc := range []struct {
		name, kind, extra, expression string
		ready                         bool
	}{
		{"source", "longtext", "STORED GENERATED", TraceProjectionMetadataExpression, true},
		{"engine_quoted_paths", "longtext", "STORED GENERATED", strings.ReplaceAll(TraceProjectionMetadataExpression, `"`, `\"`), true},
		{"ordinary", "longtext", "", TraceProjectionMetadataExpression, false},
		{"virtual", "longtext", "VIRTUAL GENERATED", TraceProjectionMetadataExpression, false},
		{"short_type", "text", "STORED GENERATED", TraceProjectionMetadataExpression, false},
		{"missing_null_io", "longtext", "STORED GENERATED", strings.Replace(TraceProjectionMetadataExpression, "'attributes.gen_ai.input.messages',NULL,", "", 1), false},
		{"changed_path", "longtext", "STORED GENERATED", strings.Replace(TraceProjectionMetadataExpression, `$."usage"`, `$."other"`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", TraceProjectionMetadataColumn).WillReturnRows(projectionSummaryColumnRows(tc.kind, tc.extra, tc.expression))
			if tc.ready {
				mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", TraceProjectionContentColumn).WillReturnRows(projectionSummaryColumnRows("longtext", "STORED GENERATED", TraceProjectionContentExpression))
			}
			ready, err := TraceProjectionSummaryColumnsReady(context.Background(), db, "test")
			require.NoError(t, err)
			require.Equal(t, tc.ready, ready)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestProjectionSummaryMigrationUsesOneAlterForOnlyMissingColumns(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		existing             map[string]bool
		completedBeforeLease bool
	}{
		{"all_missing", map[string]bool{}, false},
		{"partial_existing", map[string]bool{TraceProjectionMetadataColumn: true, TraceProjectionMetricsVersionColumn: true}, false},
		{"completed_before_lease", map[string]bool{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			probe := "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION"
			for _, column := range traceProjectionSummaryColumns() {
				expect := mock.ExpectQuery(probe).WithArgs("test_trace_projections", column[0])
				if tc.existing[column[0]] {
					expect.WillReturnRows(projectionSummaryColumnRows("longtext", "STORED GENERATED", column[2]))
				} else {
					expect.WillReturnError(sql.ErrNoRows)
				}
			}
			expectProjectionSummaryMigrationLease(mock)
			additions := []string{}
			// Recheck every definition under the lease before any table backfill.
			for _, column := range traceProjectionSummaryColumns() {
				expect := mock.ExpectQuery(probe).WithArgs("test_trace_projections", column[0])
				if tc.existing[column[0]] || tc.completedBeforeLease {
					expect.WillReturnRows(projectionSummaryColumnRows("longtext", "STORED GENERATED", column[2]))
				} else {
					expect.WillReturnError(sql.ErrNoRows)
					additions = append(additions, "ADD COLUMN "+column[0]+" "+column[1]+" GENERATED ALWAYS AS ("+column[2]+") STORED")
				}
			}
			if len(additions) > 0 {
				mock.ExpectExec("^" + regexp.QuoteMeta("ALTER TABLE `test_trace_projections` "+strings.Join(additions, ",")) + "$").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			for _, column := range traceProjectionSummaryColumns() {
				mock.ExpectQuery(probe).WithArgs("test_trace_projections", column[0]).WillReturnRows(projectionSummaryColumnRows("longtext", "STORED GENERATED", column[2]))
			}
			expectProjectionSummaryMigrationRelease(mock)
			require.NoError(t, InitializeTraceProjectionSummaries(context.Background(), db, "test"))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func expectProjectionSummaryMigrationLease(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT generation FROM .*enabled=TRUE.*suspended=FALSE.*rebuild_required=FALSE").WithArgs(ProjectionSchemaVersion).WillReturnRows(sqlmock.NewRows([]string{"generation"}).AddRow(7))
	mock.ExpectExec("UPDATE .*SET migration_owner=\\?,migration_until=UNIX_TIMESTAMP\\(\\)\\+300 WHERE").WithArgs(sqlmock.AnyArg(), ProjectionSchemaVersion, int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT @@session.ob_query_timeout").WillReturnRows(sqlmock.NewRows([]string{"timeout"}).AddRow(10000000))
	mock.ExpectExec("SET SESSION ob_query_timeout=120000000").WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectProjectionSummaryMigrationRelease(mock sqlmock.Sqlmock) {
	mock.ExpectExec("SET SESSION ob_query_timeout=10000000").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("UPDATE .*SET migration_owner='',migration_until=0 WHERE").WithArgs(sqlmock.AnyArg(), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
}

func TestProjectionSummaryMigrationRechecksConflictBeforeAnyAlter(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	probe := "SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION"
	for range 4 {
		mock.ExpectQuery(probe).WillReturnError(sql.ErrNoRows)
	}
	expectProjectionSummaryMigrationLease(mock)
	for i, column := range traceProjectionSummaryColumns() {
		expect := mock.ExpectQuery(probe).WithArgs("test_trace_projections", column[0])
		if i == 3 {
			expect.WillReturnRows(projectionSummaryColumnRows("longtext", "", "'{}'"))
		} else {
			expect.WillReturnError(sql.ErrNoRows)
		}
	}
	expectProjectionSummaryMigrationRelease(mock)
	err = InitializeTraceProjectionSummaries(context.Background(), db, "test")
	require.ErrorContains(t, err, "incompatible projection summary column "+TraceProjectionOperationPreviewsColumn)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionSummaryMigrationPreservesConflictAndDurableOff(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			if conflict {
				mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(projectionSummaryColumnRows("longtext", "", "'{}'"))
			} else {
				for range 4 {
					mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnError(sql.ErrNoRows)
				}
				mock.ExpectQuery("SELECT generation FROM").WillReturnError(sql.ErrNoRows)
			}
			err = InitializeTraceProjectionSummaries(context.Background(), db, "test")
			if conflict {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// Backfill and old-writer updates must preserve the exact former SQL output,
// including explicit null I/O, large integers, usage coverage, and complete I/O.
func TestOceanBaseProjectionSummaryColumnsBackfillAndUpdates(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated SQL DSN")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()
	prefix := fmt.Sprintf("summary_%x", time.Now().UnixNano())
	for _, suffix := range []string{"trace_projections", "projection_maintenance"} {
		suffix := suffix
		defer func() { _, e := db.Exec("DROP TABLE IF EXISTS " + tableName(prefix, suffix)); require.NoError(t, e) }()
	}
	_, err = db.Exec("CREATE TABLE " + tableName(prefix, "trace_projections") + " (id INT PRIMARY KEY,row_json LONGTEXT NOT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	require.NoError(t, err)
	require.NoError(t, ensureProjectionMaintenanceControl(ctx, db, prefix))
	_, err = db.Exec("UPDATE " + tableName(prefix, "projection_maintenance") + " SET enabled=TRUE,suspended=FALSE,rebuild_required=FALSE,schema_version=1,generation=7 WHERE id=1")
	require.NoError(t, err)
	fullInput := strings.Repeat("canonical input ", 100000) + "INPUT_TAIL"
	operationPreview := map[string]any{"name": "operation", "latency": 1, "status": 1, "input": "selected input", "output": "selected output", "input_truncated": false, "output_truncated": true}
	body, err := json.Marshal(map[string]any{"trace_id": "01", "userId": nil, "usage": map[string]any{"total": json.Number("9007199254740993")}, "coverage": map[string]any{"billableCalls": 1}, "_contentComplete": false, "attributes.gen_ai.input.messages": fullInput, "attributes.gen_ai.output.messages": "answer", "attributes.unknown.io": strings.Repeat("raw ", 100000), "_statistics": strings.Repeat("removed ", 100000), "_metrics": map[string]any{"version": "private"}, "_operation_previews": map[string]any{"op": operationPreview}})
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO "+tableName(prefix, "trace_projections")+" VALUES (1,?)", string(body))
	require.NoError(t, err)
	require.NoError(t, InitializeTraceProjectionSummaries(ctx, db, prefix))
	ready, err := TraceProjectionSummaryColumnsReady(ctx, db, prefix)
	require.NoError(t, err)
	require.True(t, ready)
	var backfilledVersion string
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionMetricsVersionColumn+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&backfilledVersion))
	require.Equal(t, "private", backfilledVersion)
	var backfilledPreviews, oldPreviews string
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionOperationPreviewsColumn+","+TraceProjectionOperationPreviewsExpression+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&backfilledPreviews, &oldPreviews))
	require.Equal(t, oldPreviews, backfilledPreviews)
	require.Contains(t, backfilledPreviews, "selected input")
	var metadata, content, oldMetadata, oldContent string
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionMetadataColumn+","+TraceProjectionContentColumn+","+TraceProjectionMetadataExpression+","+TraceProjectionContentExpression+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&metadata, &content, &oldMetadata, &oldContent))
	require.Equal(t, oldMetadata, metadata)
	require.Equal(t, oldContent, content)
	require.Less(t, len(metadata), 10000)
	require.NotContains(t, metadata, "INPUT_TAIL")
	require.Contains(t, content, "INPUT_TAIL")
	require.NotContains(t, content, "removed ")
	decoder := json.NewDecoder(strings.NewReader(metadata))
	decoder.UseNumber()
	var compact map[string]any
	require.NoError(t, decoder.Decode(&compact))
	require.Equal(t, json.Number("9007199254740993"), compact["usage"].(map[string]any)["total"])
	require.Equal(t, json.Number("1"), compact["coverage"].(map[string]any)["billableCalls"])
	require.Equal(t, false, compact["_contentComplete"])
	for _, key := range []string{"userId", "attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "attributes.gen_ai.tool.call.arguments", "attributes.gen_ai.tool.call.result"} {
		require.Contains(t, compact, key)
		require.Nil(t, compact[key])
	}
	// An old publisher only knows row_json; generated storage must update itself.
	_, err = db.Exec("UPDATE "+tableName(prefix, "trace_projections")+" SET row_json=? WHERE id=1", `{"trace_id":"01","usage":{"total":null},"coverage":{"billableCalls":0},"attributes.gen_ai.input.messages":"new tail"}`)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionMetadataColumn+","+TraceProjectionContentColumn+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&metadata, &content))
	require.Contains(t, content, "new tail")
	require.NotContains(t, metadata, "9007199254740993")
	var removedVersion sql.NullString
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionMetricsVersionColumn+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&removedVersion))
	require.False(t, removedVersion.Valid)
	var removedPreviews sql.NullString
	require.NoError(t, db.QueryRow("SELECT "+TraceProjectionOperationPreviewsColumn+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=1").Scan(&removedPreviews))
	require.False(t, removedPreviews.Valid)
	// Missing, null, boolean and literal-string completeness flags retain the
	// legacy fallback decision after the metadata projection inserts null keys.
	flags := []struct {
		document string
		want     int
	}{
		{`{}`, 1}, {`{"_contentComplete":false}`, 0}, {`{"_contentComplete":"false"}`, 0}, {`{"_contentComplete":true}`, 1}, {`{"_contentComplete":"False"}`, 1}, {`{"_contentComplete":"false "}`, 1}, {`{"_contentComplete":null}`, 1},
	}
	for i, fixture := range flags {
		_, err = db.Exec("INSERT INTO "+tableName(prefix, "trace_projections")+" (id,row_json) VALUES (?,?)", i+2, fixture.document)
		require.NoError(t, err)
		proof := func(column string) string {
			return "CASE WHEN BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(" + column + ",'$._contentComplete')),'true')=BINARY 'false' THEN 0 ELSE 1 END"
		}
		var compactFlag, rawFlag int
		require.NoError(t, db.QueryRow("SELECT "+proof(TraceProjectionMetadataColumn)+","+proof("row_json")+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=?", i+2).Scan(&compactFlag, &rawFlag))
		require.Equal(t, fixture.want, compactFlag)
		require.Equal(t, rawFlag, compactFlag)
	}
	versions := []string{`{}`, `{"_metrics":null}`, `{"_metrics":{"version":null}}`, `{"_metrics":{"version":"native-trace-metrics-v1"}}`, `{"_metrics":{"version":"NATIVE-TRACE-METRICS-V1"}}`, `{"_metrics":{"version":"native-trace-metrics-v1 "}}`, `{"_metrics":{"version":["native-trace-metrics-v1"]}}`, `{"_metrics":{"version":7}}`}
	for i, document := range versions {
		_, err = db.Exec("INSERT INTO "+tableName(prefix, "trace_projections")+" (id,row_json) VALUES (?,?)", i+20, document)
		require.NoError(t, err)
		var persisted, legacy sql.NullString
		var persistedMismatch, legacyMismatch bool
		predicate := func(expression string) string {
			return "BINARY COALESCE(" + expression + ",'')<>BINARY 'native-trace-metrics-v1'"
		}
		require.NoError(t, db.QueryRow("SELECT "+TraceProjectionMetricsVersionColumn+","+TraceProjectionMetricsVersionExpression+","+predicate(TraceProjectionMetricsVersionColumn)+","+predicate(TraceProjectionMetricsVersionExpression)+" FROM "+tableName(prefix, "trace_projections")+" WHERE id=?", i+20).Scan(&persisted, &legacy, &persistedMismatch, &legacyMismatch))
		require.Equal(t, legacy, persisted)
		require.Equal(t, legacyMismatch, persistedMismatch)
		require.Equal(t, i != 3, persistedMismatch)
	}
	var enabled bool
	var generation int64
	require.NoError(t, db.QueryRow("SELECT enabled,generation FROM "+tableName(prefix, "projection_maintenance")+" WHERE id=1").Scan(&enabled, &generation))
	require.True(t, enabled)
	require.EqualValues(t, 7, generation)
	require.NoError(t, InitializeTraceProjectionSummaries(ctx, db, prefix))
}
