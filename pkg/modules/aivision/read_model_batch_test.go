package aivision

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestProjectionBatchValidatesEntireInputBeforeAnyReadOrWrite(t *testing.T) {
	store := &oceanBaseStore{}
	_, e := store.traceSourceBatch(context.Background(), "org", "space", []string{"abc", "abc"})
	require.Error(t, e)
	_, e = store.traceSourceBatch(context.Background(), "org", "space", make([]string, 101))
	require.Error(t, e)
	valid := validProjection()
	invalid := validProjection()
	invalid.SourceRevision = 0
	_, e = store.publishTraceBatch(context.Background(), "org", []traceProjection{valid, invalid})
	require.Error(t, e)
	_, e = store.publishTraceBatch(context.Background(), "org", []traceProjection{valid, valid})
	require.Error(t, e)
}

func TestProjectionBatchKeepsPerTraceCASAndReceipts(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	first := validProjection()
	first.UserID = "owner"
	first.AgentProduct = "product"
	second := first
	second.TraceID = "abc"
	for _, p := range []traceProjection{first, second} {
		mock.ExpectBegin()
		expectProjectionMaintenance(mock, true, true)
		mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", p.SpaceID, p.TraceID).WillReturnRows(sqlmock.NewRows([]string{"revision", "user", "product", "valid"}).AddRow(2, "owner", "product", true))
		mock.ExpectRollback()
	}
	// The second row is validated before the first is attempted, including its
	// canonical identity. Make it valid while preserving a distinct ID.
	second.Row = []byte(`{"trace_id":"abc","start_time_unix_nano":"100000000"}`)
	result, e := store.publishTraceBatch(context.Background(), "org", []traceProjection{first, second})
	require.NoError(t, e)
	r := result.(map[string]any)
	require.Equal(t, "native-trace-publish-batch-v1", r["contract"])
	data := r["data"].([]any)
	for i, p := range []traceProjection{first, second} {
		receipt := data[i].(map[string]any)
		require.Equal(t, p.TraceID, receipt["trace_id"])
		require.Equal(t, "source_revision_changed", receipt["reason"])
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionSourceBatchRetainsUnsupportedPerTraceReceipts(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT trace_id,source_revision,min_timestamp").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "min_timestamp", "max_timestamp", "user_id", "agent_product", "scope_valid"}))
	mock.ExpectCommit()
	result, e := store.traceSourceBatch(context.Background(), "org", "space", []string{"abc", "def"})
	require.NoError(t, e)
	r := result.(map[string]any)
	require.Equal(t, "native-trace-source-batch-v1", r["contract"])
	data := r["data"].([]any)
	require.Len(t, data, 2)
	for i, item := range data {
		receipt := item.(map[string]any)
		require.Equal(t, []string{"abc", "def"}[i], receipt["trace_id"])
		require.Equal(t, "source_not_enrolled", receipt["reason"])
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionSourceBatchUsesFourQueriesAndOneSnapshot(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, 7, 1))
	mock.ExpectQuery("SELECT trace_id,source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "min_timestamp", "max_timestamp", "user_id", "agent_product", "scope_valid"}).AddRow("abc", 1, 100000000, 100000000, "owner", "product", true).AddRow("def", 2, 100000000, 100000000, "owner", "product", false))
	mock.ExpectQuery("SELECT trace_id,COUNT.*source_bytes").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "source_bytes", "maximum_span_bytes"}).AddRow("abc", 1, 3000, 3000).AddRow("def", 1, 3000, 3000))
	rows := windowSpanRows()
	addWindowSpan(rows, "abc")
	rows.AddRow("def", "span", `{}`, `{invalid`, `[]`, `[]`, `{}`, "100000000", "100000001", 0)
	mock.ExpectQuery("SELECT trace_id,").WillReturnRows(rows)
	mock.ExpectCommit()
	response, e := store.traceSourceBatch(context.Background(), "org", "space", []string{"abc", "def", "aaa"})
	require.NoError(t, e)
	result := response.(map[string]any)
	require.True(t, result["supported"].(bool))
	data := result["data"].([]any)
	require.Len(t, data, 3)
	first := data[0].(map[string]any)
	require.Equal(t, "abc", first["trace_id"])
	require.EqualValues(t, 7, first["maintenance_generation"])
	require.EqualValues(t, 1, first["source_revision"])
	require.Len(t, first["spans"], 1)
	require.Equal(t, "source_json_invalid", data[1].(map[string]any)["reason"])
	require.Equal(t, "source_not_enrolled", data[2].(map[string]any)["reason"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionSourceBatchPreflightAndIncompleteReceipts(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		firstBytes, secondBytes int64
		firstCount              int
		reason                  string
		raw                     bool
	}{{"whole_batch_bytes", 9 * 1024 * 1024, 9 * 1024 * 1024, 1, "source_batch_payload_limit", false}, {"one_oversized_source", 17 * 1024 * 1024, 3000, 1, "source_snapshot_payload_limit", true}, {"one_incomplete_source", 3000, 3000, 2, "source_snapshot_incomplete", true}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, e := sqlmock.New()
			require.NoError(t, e)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			mock.ExpectQuery("SELECT trace_id,source_revision").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision", "min_timestamp", "max_timestamp", "user_id", "agent_product", "scope_valid"}).AddRow("abc", 1, 100, 200, "owner-a", "product-a", true).AddRow("def", 9, 300, 400, "owner-b", "product-b", false))
			mock.ExpectQuery("SELECT trace_id,COUNT").WillReturnRows(sqlmock.NewRows([]string{"trace_id", "span_count", "source_bytes", "maximum_span_bytes"}).AddRow("abc", tc.firstCount, tc.firstBytes, 3000).AddRow("def", 1, tc.secondBytes, 3000))
			if tc.raw {
				rows := windowSpanRows()
				if tc.firstCount == 2 {
					addWindowSpan(rows, "abc")
				}
				addWindowSpan(rows, "def")
				mock.ExpectQuery("SELECT trace_id,").WillReturnRows(rows)
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			response, e := store.traceSourceBatch(context.Background(), "org", "space", []string{"abc", "def"})
			require.NoError(t, e)
			result := response.(map[string]any)
			if !tc.raw {
				require.Equal(t, readModelUnsupported(tc.reason), result)
			} else {
				data := result["data"].([]any)
				require.Equal(t, tc.reason, data[0].(map[string]any)["reason"])
				second := data[1].(map[string]any)
				require.True(t, second["supported"].(bool))
				require.Equal(t, "owner-b", second["source_user_id"])
				require.Equal(t, "product-b", second["source_agent_product"])
				require.False(t, second["scope_valid"].(bool))
				require.EqualValues(t, 9, second["source_revision"])
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestProjectionSourceBatchDatabaseFailureIsNotPartialSuccess(t *testing.T) {
	db, mock, e := sqlmock.New()
	require.NoError(t, e)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT trace_id,source_revision").WillReturnError(fmt.Errorf("source metadata permission denied"))
	mock.ExpectRollback()
	result, e := store.traceSourceBatch(context.Background(), "org", "space", []string{"abc", "def"})
	require.ErrorContains(t, e, "permission denied")
	require.Nil(t, result)
	require.NoError(t, mock.ExpectationsWereMet())
}
