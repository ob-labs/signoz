package aivision

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

// A small selected summary must stay usable even when the source projection
// contains arbitrarily large removable I/O. The query boundary must read only
// the stored summary; touching row_json would parse the source before budgeting.
func TestSessionStoredSummaryPreservesCompleteGroupsWithoutRawJSON(t *testing.T) {
	for _, includeContent := range []bool{false, true} {
		t.Run(map[bool]string{false: "metadata", true: "content"}[includeContent], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			require.NoError(t, err)
			column := "metadata_json_v1"
			if includeContent {
				column = "content_json_v1"
			}
			mock.ExpectQuery(`SELECT p\.session_id,COUNT\(\*\).*OCTET_LENGTH\(p\.`+column+`\)`).WithArgs("org", "s", "s").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("s", 2, 2000))
			input := "complete content after the preview tail"
			mock.ExpectQuery(`SELECT p\.session_id AS session_id,p\.trace_id AS trace_id,p\.`+column+` AS row_json`).WithArgs("org", "s", "s", sessionPageTraceLimit+1).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_id", "row_json", "preview_json", "source_revision", "matched"}).AddRow("s", "01", `{"trace_id":"01","attributes.gen_ai.input.messages":null}`, `{}`, 1, 0).AddRow("s", "02", `{"trace_id":"02","attributes.gen_ai.input.messages":"`+input+`"}`, `{}`, 1, 1))
			mock.ExpectRollback()
			rows, reason, err := sessionPageBatchRows(context.Background(), tx, "projections", "p.org_id=?", []any{"org"}, "TRUE", nil, []map[string]any{{"session_id": "s", "trace_count": int64(2)}}, 20, includeContent)
			require.NoError(t, err)
			require.Empty(t, reason)
			require.Len(t, rows["s"], 2)
			require.Equal(t, int64(0), rows["s"][0]["matched"])
			require.True(t, strings.Contains(rows["s"][1]["row_json"].(string), input))
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSessionStoredSummaryBudgetRejectsBeforeJSONFetch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT p\.session_id,COUNT\(\*\).*OCTET_LENGTH\(p\.metadata_json_v1\)`).WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "selected_bytes"}).AddRow("s", 1, sessionPagePayloadLimit+1))
	mock.ExpectRollback()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	rows, reason, err := sessionPageBatchRows(context.Background(), tx, "projections", "TRUE", nil, "TRUE", nil, []map[string]any{{"session_id": "s", "trace_count": int64(1)}}, 20, false)
	require.NoError(t, err)
	require.Nil(t, rows)
	require.Equal(t, "session_page_payload_limit", reason)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectProjectionSummaryCapability(mock sqlmock.Sqlmock) {
	for _, column := range [][2]string{{ingest.TraceProjectionMetadataColumn, ingest.TraceProjectionMetadataExpression}, {ingest.TraceProjectionContentColumn, ingest.TraceProjectionContentExpression}} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", column[0]).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", column[1]))
	}
}

func TestCanonicalPagesDeclineUnverifiedSummaryColumns(t *testing.T) {
	for _, kind := range []string{"trace", "session"} {
		for _, definition := range []string{"missing", "ordinary", "virtual", "conflicting"} {
			t.Run(kind+"_"+definition, func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
				mock.ExpectBegin()
				expectProjectionMaintenance(mock, true, false)
				rows := sqlmock.NewRows([]string{"kind", "extra", "expression"})
				switch definition {
				case "ordinary":
					rows.AddRow("longtext", "", ingest.TraceProjectionMetadataExpression)
				case "virtual":
					rows.AddRow("longtext", "VIRTUAL GENERATED", ingest.TraceProjectionMetadataExpression)
				case "conflicting":
					rows.AddRow("longtext", "STORED GENERATED", "'{}'")
				}
				mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnRows(rows)
				mock.ExpectRollback()
				var result any
				if kind == "trace" {
					result, err = store.readModelPage(context.Background(), "org", sessionReadModelTestParams().nativeListParams)
				} else {
					result, err = store.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
				}
				require.NoError(t, err)
				require.Equal(t, readModelUnsupported("projection_summary_columns_unavailable"), result)
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestCanonicalStoredSummaryBudgetStopsBeforeFetch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	expectProjectionSummaryCapability(mock)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "content"}).AddRow(0, 0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery(`SELECT COALESCE\(SUM\(selected_bytes\),0\) FROM \(SELECT OCTET_LENGTH\(p\.metadata_json_v1\).*LIMIT \? OFFSET \?\) selected_page`).WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(projectionBatchPayloadLimit + 1))
	mock.ExpectRollback()
	p := sessionReadModelTestParams().nativeListParams
	p.MetadataOnly = true
	result, err := store.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_page_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCanonicalSummaryInspectionFailureIsNotSuccessfulFallback(t *testing.T) {
	for _, kind := range []string{"trace", "session"} {
		t.Run(kind, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WillReturnError(sql.ErrConnDone)
			mock.ExpectRollback()
			var result any
			if kind == "trace" {
				result, err = store.readModelPage(context.Background(), "org", sessionReadModelTestParams().nativeListParams)
			} else {
				result, err = store.sessionReadModelPage(context.Background(), "org", sessionReadModelTestParams())
			}
			require.ErrorIs(t, err, sql.ErrConnDone)
			require.Nil(t, result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
