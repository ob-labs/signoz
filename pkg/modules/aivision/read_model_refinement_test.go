package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

func expectContentCapability(mock sqlmock.Sqlmock, ready bool) {
	rows := sqlmock.NewRows([]string{"COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"})
	if ready {
		rows.AddRow("tinyint", "STORED GENERATED", ingest.TraceContentCompleteExpression)
	}
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceContentCompleteColumn).WillReturnRows(rows)
}

func refinementCoverageMatcher(expected, actual string) error {
	if expected == "typed_content_coverage" {
		for _, proof := range []string{"p.content_complete_v1=0", "s.scope_valid=FALSE", "p.source_revision<>s.source_revision", "p.projection_context<>?", "s.min_timestamp<?", "BINARY p.agent_uuid=BINARY ?"} {
			if !strings.Contains(actual, proof) {
				return fmt.Errorf("missing coverage proof %s", proof)
			}
		}
		if strings.Contains(actual, "row_json") {
			return fmt.Errorf("typed coverage still parses a LOB")
		}
		return nil
	}
	return metadataCoverageMatcher(expected, actual)
}

func TestTraceAndSessionTypedContentCoveragePreservesIncompleteDecline(t *testing.T) {
	for _, kind := range []string{"trace", "session"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(refinementCoverageMatcher)))
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			if kind == "trace" {
				expectProjectionMaintenance(mock, true, false)
			}
			mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
			expectContentCapability(mock, true)
			mock.ExpectQuery("typed_content_coverage").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "content"}).AddRow(0, 0, 1))
			mock.ExpectRollback()
			p := sessionReadModelTestParams()
			p.Keyword, p.SearchType = "missing-tail", "input"
			if kind == "trace" {
				result, err := store.readModelPage(context.Background(), "org", p.nativeListParams)
				require.NoError(t, err)
				require.Equal(t, readModelUnsupported("content_projection_incomplete"), result)
			} else {
				tx, err := db.Begin()
				require.NoError(t, err)
				reason, err := store.sessionReadModelCoverage(context.Background(), tx, "org", p)
				require.NoError(t, err)
				require.Equal(t, "content_projection_incomplete", reason)
				require.NoError(t, tx.Rollback())
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTraceTokenMinimumDeclinesMissingGeneratedCapabilityBeforeCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}))
	mock.ExpectRollback()
	p := sessionReadModelTestParams().nativeListParams
	p.MetadataOnly = true
	p.Filters = []nativeFilter{{Field: "total_tokens", Op: ">=", Value: 100}}
	result, err := store.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("session_token_columns_missing"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTraceTokenMinimumUsesVerifiedColumnsForPageAndFilteredCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr))
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "content"}).AddRow(0, 0, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM .*p.session_trace_tokens_v1>=\?`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(`SELECT JSON_OBJECT.*p.session_trace_tokens_v1>=\?.*LIMIT \? OFFSET \?`).WillReturnRows(sqlmock.NewRows([]string{"row_json", "preview_json", "source_revision"}))
	for range 4 {
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}))
	}
	mock.ExpectCommit()
	p := sessionReadModelTestParams().nativeListParams
	p.MetadataOnly = true
	p.Filters = []nativeFilter{{Field: "total_tokens", Op: ">=", Value: 100}}
	result, err := store.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, true, result.(map[string]any)["supported"])
	require.Equal(t, "not_requested", result.(map[string]any)["meta"].(map[string]any)["contentCoverageStrategy"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestContentCoverageInspectionFailureIsNotAnEmptyPage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()
	result, err := store.readModelPage(context.Background(), "org", sessionReadModelTestParams().nativeListParams)
	require.ErrorIs(t, err, sql.ErrConnDone)
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTypedContentCoverageAvoidsLOBWithoutWeakeningAgentCheck(t *testing.T) {
	sql := projectionIncompleteContentSQL(true, true)
	require.Contains(t, sql, "BINARY p.agent_uuid=BINARY ?")
	require.Contains(t, sql, "p.content_complete_v1=0")
	require.NotContains(t, sql, "row_json")
	require.Contains(t, projectionIncompleteContentSQL(true, false), "JSON_EXTRACT")
	require.Equal(t, "0", projectionIncompleteContentSQL(false, true))
}

func TestTraceTokenPositiveMinimumUsesStoredValueAndRejectsUnprovenZeroSemantics(t *testing.T) {
	p := sessionReadModelTestParams().nativeListParams
	for _, field := range []string{"total_tokens", "attributes.gen_ai.usage.total_tokens"} {
		p.Filters = []nativeFilter{{Field: field, Op: ">=", Value: 100}}
		sql, args, err := readModelWhere("org", p)
		require.NoError(t, err)
		require.Contains(t, sql, "p.session_trace_tokens_v1>=?")
		require.NotContains(t, sql, "JSON_EXTRACT")
		require.EqualValues(t, 100, args[len(args)-1])
		for _, value := range []any{0, 99, -1, 100.5, true} {
			p.Filters[0].Value = value
			_, _, err := readModelWhere("org", p)
			require.Error(t, err)
		}
		p.Filters[0].Value = 100
		p.Filters[0].Op = "="
		_, _, err = readModelWhere("org", p)
		require.Error(t, err)
	}
	// Capability detection is needed only for the new minimum; metadata's
	// existing typed predicates do not acquire a Token dependency.
	p.Filters = []nativeFilter{{Field: "status_code", Op: "=", Value: 0}}
	sql, _, err := readModelWhere("org", p)
	require.NoError(t, err)
	require.False(t, strings.Contains(sql, "trace_tokens"))
}
