package aivision

import (
	"encoding/json"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReadModelDeadlinesSeparateOperatorRecoveryFromQueries(t *testing.T) {
	for _, action := range []string{"page", "window", "sessions/page", "dirty", "backfill", "batch-publish"} {
		require.Equal(t, 30*time.Second, readModelRequestTimeout(action, 30*time.Second))
	}
	require.Equal(t, 2*time.Minute, readModelRequestTimeout("maintenance/resume", 30*time.Second))
}

func TestStandardSessionQueryIgnoresRetiredStrategyFlags(t *testing.T) {
	for _, key := range []string{"SIGNOZ_AIVISION_SESSION_TOKEN_QUERY_OPTIMIZATION_ENABLED", "SIGNOZ_AIVISION_SESSION_PAGE_BATCH_ENABLED", "SIGNOZ_AIVISION_SESSION_TOKEN_GENERATED_COLUMNS_ENABLED", "SIGNOZ_AIVISION_TRACE_READ_MODEL_ENABLED", "SIGNOZ_AIVISION_TRACE_READ_MODEL_TRACKING_ENABLED", "SIGNOZ_AIVISION_TRACE_READ_MODEL_AUTHORITATIVE_INGEST"} {
		t.Setenv(key, "false")
	}
	store := NewModule(factory.ProviderSettings{}, nil, nil, telemetrystore.Config{}).store
	p := sessionReadModelTestParams()
	zero := int64(0)
	p.MinTotalTokens = &zero
	require.Equal(t, "json_table", store.sessionTokenQueryStrategy(p))
}

func TestProjectionResumeRequiresDeploymentOperatorSecret(t *testing.T) {
	r := httptest.NewRequest("POST", "/maintenance/resume", nil)
	t.Setenv("SIGNOZ_INGEST_INTERNAL_KEY", "")
	require.False(t, projectionResumeOperatorAuthorized(r))
	t.Setenv("SIGNOZ_INGEST_INTERNAL_KEY", "operator-secret")
	require.False(t, projectionResumeOperatorAuthorized(r))
	r.Header.Set("X-Signoz-Ingest-Key", "wrong")
	require.False(t, projectionResumeOperatorAuthorized(r))
	r.Header.Set("X-Signoz-Ingest-Key", "operator-secret")
	require.True(t, projectionResumeOperatorAuthorized(r))
}

func TestProjectionResumeHandlerRejectsMissingOrWrongDeploymentKeyWithForbidden(t *testing.T) {
	t.Setenv("SIGNOZ_INGEST_INTERNAL_KEY", "operator-secret")
	for _, tc := range []struct{ name, header string }{{"missing", ""}, {"wrong", "wrong-key"}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			cfg := config{Enabled: true, QueryTimeout: 30 * time.Second}
			h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/ai-vision/traces/read-model/maintenance/resume", nil)
			request = request.WithContext(authtypes.NewContextWithClaims(request.Context(), authtypes.Claims{OrgID: "00000000-0000-4000-8000-000000000001"}))
			if tc.header != "" {
				request.Header.Set("X-Signoz-Ingest-Key", tc.header)
			}
			response := httptest.NewRecorder()
			h.ReadModel(response, request)
			require.Equal(t, http.StatusForbidden, response.Code)
			var body struct {
				Status string `json:"status"`
				Error  struct {
					Type string `json:"type"`
					Code string `json:"code"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			require.Equal(t, "error", body.Status)
			require.Equal(t, "forbidden", body.Error.Type)
			require.Equal(t, "forbidden", body.Error.Code)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
