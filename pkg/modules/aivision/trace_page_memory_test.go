package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func pageMemoryStore(t *testing.T) (*oceanBaseStore, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return &oceanBaseStore{db: db, cfg: config{TablePrefix: "test", MaxLimit: 10000}}, mock
}
func pageMemoryReady(mock sqlmock.Sqlmock) {
	for _, col := range [][3]string{{ingest.TracePageDimensionsColumn, "longtext", ingest.TracePageDimensionsExpression}, {ingest.TracePageSourceBytesColumn, "bigint", ingest.TracePageSourceBytesExpression}, {ingest.TracePageWireBytesColumn, "bigint", ingest.TracePageWireBytesExpression}} {
		mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_traces", col[0]).WillReturnRows(sqlmock.NewRows([]string{"type", "extra", "expression"}).AddRow(col[1], "STORED GENERATED", col[2]))
	}
}
func TestTracePageBudgetStopsBeforeRawAggregation(t *testing.T) {
	for _, metadata := range []bool{true, false} {
		store, mock := pageMemoryStore(t)
		mock.ExpectBegin()
		pageMemoryReady(mock)
		if metadata {
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max"}).AddRow(1, 1<<20, 1<<20))
		} else {
			mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max"}).AddRow(1, 512, 512))
			mock.ExpectQuery("WITH base").WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(25 * 1024 * 1024))
		}
		mock.ExpectRollback()
		result, err := store.tracePage(context.Background(), "org", nativeListParams{From: 1790985600000, To: 1791244800000, Page: 1, Limit: 50, SpaceID: "space"})
		require.True(t, errors.Is(err, errTracePageBudget), err)
		require.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

// Opt-in export for the reproducible external memory harness. It exports the
// actual production builder, with no reimplemented filtering or SQL adapters.
func TestExportTracePageMemorySQL(t *testing.T) {
	out := os.Getenv("SIGNOZ_TEST_MEMORY_SQL_OUT")
	if out == "" {
		t.Skip("set explicit output directory")
	}
	p := nativeListParams{From: 1790985600000, To: 1791244800000, Page: 1, Limit: 50, SpaceID: "memory-retest", User: "subject", AgentProduct: "personal", AgentUUID: "agent-a"}
	query, args, err := buildTracePageSQL("signoz_traces", "memory-retest", p, 10000)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out, "fixed-trace-page.sql"), []byte(query), 0600))
	encoded, err := json.Marshal(args)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(out, "fixed-trace-page-args.json"), encoded, 0600))
}

func TestTraceQueryAdmissionIsSharedAndReleased(t *testing.T) {
	first, second, third := httptest.NewRecorder(), httptest.NewRecorder(), httptest.NewRecorder()
	require.True(t, acquireTraceQuery(first))
	require.True(t, acquireTraceQuery(second))
	defer func() { releaseTraceQuery(); releaseTraceQuery() }()
	require.False(t, acquireTraceQuery(third))
	require.Equal(t, 429, third.Code)
	require.Equal(t, "1", third.Header().Get("Retry-After"))
}

func TestOceanBaseTracePageDimensionsBackfillAndUpdate(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set real database DSN")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	prefix := fmt.Sprintf("sn_dim_%d", time.Now().UnixNano())
	table := "`" + prefix + "_traces`"
	_, err = db.Exec("CREATE TABLE " + table + " LIKE signoz_traces")
	require.NoError(t, err)
	defer db.Exec("DROP TABLE " + table)
	for _, column := range []string{ingest.TracePageDimensionsColumn, ingest.TracePageSourceBytesColumn, ingest.TracePageWireBytesColumn} {
		var exists int
		require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME=?", prefix+"_traces", column).Scan(&exists))
		if exists > 0 {
			_, err = db.Exec("ALTER TABLE " + table + " DROP COLUMN " + column)
			require.NoError(t, err)
		}
	}
	fixture := tracePageIntegrationFixture{db: db, table: table, org: "test", start: time.Now()}
	root, _ := fixture.pair("space", 1)
	root.attributes = map[string]any{"ant.username": "old"}
	fixture.insert(t, []tracePageIntegrationSpan{root})
	ready, err := ingest.TracePageDimensionsReady(context.Background(), db, prefix)
	require.NoError(t, err)
	require.False(t, ready)
	require.NoError(t, ingest.InitializeTracePageDimensions(context.Background(), db, prefix))
	check := func(want string) {
		var raw string
		var stored, actual, bound, encoded int64
		require.NoError(t, db.QueryRow("SELECT page_dimensions_v1,page_source_bytes_v1,"+nativeSpanSourceBytesSQL()+",page_wire_bytes_v1,OCTET_LENGTH("+tracePageSpanJSON()+") FROM "+table+" b").Scan(&raw, &stored, &actual, &bound, &encoded))
		require.Equal(t, actual, stored)
		require.GreaterOrEqual(t, bound, encoded)
		var metadata map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &metadata))
		require.Equal(t, want, metadata["attribute_username"])
	}
	check("old")
	_, err = db.Exec("UPDATE "+table+" SET attributes=?,payload=?", `{"langfuse.trace.tags":null}`, `{"attributes":{"ant.username":"fallback","langfuse.trace.tags":["shadowed"]}}`)
	require.NoError(t, err)
	check("fallback")
	var tags string
	require.NoError(t, db.QueryRow("SELECT JSON_EXTRACT(page_dimensions_v1,'$.tags') FROM "+table).Scan(&tags))
	require.Equal(t, "null", tags)
	require.NoError(t, ingest.InitializeTracePageDimensions(context.Background(), db, prefix))
}
