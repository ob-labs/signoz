package aivision

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

func TestProjectionResumeInitializesStatisticsBeforeOptionalContentFailure(t *testing.T) {
	t.Setenv("SIGNOZ_INGEST_INTERNAL_KEY", "test-operator-key")
	t.Setenv("SIGNOZ_AIVISION_PROJECTION_MAINTENANCE_ENABLED", "true")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT IGNORE INTO .*projection_maintenance").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	mock.ExpectQuery("SELECT COUNT.*information_schema.TABLES").WithArgs("test_trace_sources", "test_trace_projections", "test_trace_projection_coverage").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.SessionTokenGeneratedValueColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("decimal(65,0)", "STORED GENERATED", ingest.SessionTokenGeneratedValueExpr))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.SessionTokenGeneratedBillableColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("tinyint", "STORED GENERATED", ingest.SessionTokenGeneratedBillableExpr))
	mock.ExpectCommit()
	// Ordered expectations exercise the real handler: full statistics
	// readiness must finish before a content failure can consume its context.
	mock.ExpectQuery("SELECT VERSION").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-OceanBase_CE-v4.4.2.1"))
	mock.ExpectQuery("SELECT COUNT.*information_schema.TABLES").WithArgs("test_trace_statistics", "test_call_statistics").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.StatisticsReceiptColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("char(64)", "STORED GENERATED", ingest.StatisticsReceiptExpression))
	mock.ExpectQuery("SELECT COUNT.*information_schema.COLUMNS").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(31))
	mock.ExpectQuery("SELECT COUNT.*information_schema.STATISTICS").WillReturnRows(sqlmock.NewRows([]string{"primary", "matching"}).AddRow(7, 7))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceContentCompleteColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("tinyint", "", "1"))
	expectProjectionSummaryCapability(mock)
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionMetricsVersionColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", ingest.TraceProjectionMetricsVersionExpression))
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionOperationPreviewsColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", ingest.TraceProjectionOperationPreviewsExpression))
	cfg := config{Enabled: true, QueryTimeout: time.Second, TablePrefix: "test"}
	h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/ai-vision/traces/read-model/maintenance/resume", nil)
	r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	r.Header.Set("X-Signoz-Ingest-Key", "test-operator-key")
	w := httptest.NewRecorder()
	h.ReadModel(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"resumed":true`)
	require.NoError(t, mock.ExpectationsWereMet())
}
