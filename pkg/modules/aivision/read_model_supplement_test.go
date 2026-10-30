package aivision

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

const supplementTestOrg = "00000000-0000-4000-8000-000000000001"

func runDirtySupplementRequest(t *testing.T, suffix string, setup func(sqlmock.Sqlmock)) *httptest.ResponseRecorder {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	setup(mock)
	cfg := config{Enabled: true, QueryTimeout: 30 * time.Second, TablePrefix: "test"}
	h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/dirty?space_id=space&projection_context="+strings.Repeat("a", 64)+"&limit=1"+suffix, nil)
	request = request.WithContext(authtypes.NewContextWithClaims(request.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	response := httptest.NewRecorder()
	h.ReadModel(response, request)
	require.NoError(t, mock.ExpectationsWereMet())
	return response
}

func TestDirtySupplementKeepsOriginalQueryAndAddsOnlyRequestedCandidates(t *testing.T) {
	for _, required := range []string{"", "native-trace-metrics-v1"} {
		t.Run(required, func(t *testing.T) {
			suffix := ""
			if required != "" {
				suffix = "&required_supplement=" + required
			}
			response := runDirtySupplementRequest(t, suffix, func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				expectProjectionMaintenance(mock, true, false)
				if required != "" {
					mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", ingest.TraceProjectionMetricsVersionColumn).WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}).AddRow("longtext", "STORED GENERATED", ingest.TraceProjectionMetricsVersionExpression))
				}
				query := "SELECT s.trace_id,s.source_revision FROM `test_trace_sources` s LEFT JOIN `test_trace_projections` p ON p.org_id=s.org_id AND p.space_id=s.space_id AND p.trace_id=s.trace_id WHERE s.org_id=? AND s.space_id=? AND (s.scope_valid=FALSE OR s.dirty=TRUE OR p.trace_id IS NULL OR p.source_revision<>s.source_revision OR p.semantics_version<>1 OR p.projection_context<>?"
				if required != "" {
					query += " OR BINARY COALESCE(p.metrics_version_v1,'')<>BINARY ?"
				}
				query += ") AND s.trace_id>? ORDER BY BINARY s.trace_id LIMIT ?"
				expectation := mock.ExpectQuery(regexp.QuoteMeta(query))
				if required == "" {
					expectation.WithArgs(supplementTestOrg, "space", strings.Repeat("a", 64), "", 2)
				} else {
					expectation.WithArgs(supplementTestOrg, "space", strings.Repeat("a", 64), required, "", 2)
				}
				expectation.WillReturnRows(sqlmock.NewRows([]string{"trace_id", "source_revision"}).AddRow("abc", 4).AddRow("def", 9))
				mock.ExpectCommit()
			})
			require.Equal(t, http.StatusOK, response.Code)
			var body struct {
				Data map[string]any `json:"data"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, true, body.Data["supported"])
			require.Equal(t, true, body.Data["has_more"])
			require.Equal(t, "abc", body.Data["next_cursor"])
			require.Equal(t, []any{map[string]any{"trace_id": "abc", "source_revision": float64(4)}}, body.Data["data"])
			if required == "" {
				require.NotContains(t, body.Data, "required_supplement")
			} else {
				require.Equal(t, required, body.Data["required_supplement"])
			}
		})
	}
}

func TestDirtySupplementRejectsUnknownEmptyAndRepeatedVersionsBeforeSQL(t *testing.T) {
	for _, suffix := range []string{
		"&required_supplement=", "&required_supplement=unknown", "&required_supplement=NATIVE-TRACE-METRICS-V1",
		"&required_supplement=native-trace-metrics-v1&required_supplement=native-trace-metrics-v1",
	} {
		t.Run(suffix, func(t *testing.T) {
			response := runDirtySupplementRequest(t, suffix, func(sqlmock.Sqlmock) {})
			require.Equal(t, http.StatusBadRequest, response.Code)
		})
	}
}

func TestDirtySupplementCannotOverrideSuspendedMaintenance(t *testing.T) {
	response := runDirtySupplementRequest(t, "&required_supplement=native-trace-metrics-v1", func(mock sqlmock.Sqlmock) {
		mock.ExpectBegin()
		expectProjectionMaintenance(mock, false, false)
		mock.ExpectRollback()
	})
	require.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, false, body.Data["supported"])
	require.Equal(t, "projection_maintenance_unavailable", body.Data["reason"])
}
