package aivision

import (
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/stretchr/testify/require"
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
