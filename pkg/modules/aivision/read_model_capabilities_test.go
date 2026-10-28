package aivision

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

func runCapabilitiesRequest(method, query string) *httptest.ResponseRecorder {
	h := &handler{backend: &oceanBaseStore{}, cfg: config{Enabled: true}}
	r := httptest.NewRequest(method, "/api/v1/ai-vision/traces/read-model/capabilities"+query, nil)
	r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	w := httptest.NewRecorder()
	h.ReadModel(w, r)
	return w
}

func TestReadModelCapabilitiesRequireNoTransactionalStore(t *testing.T) {
	w := runCapabilitiesRequest(http.MethodGet, "")
	require.Equal(t, http.StatusOK, w.Code)
	var result map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Equal(t, map[string]any{"supported": true, "contract": "native-trace-performance-capabilities-v1", "content_projection": "native-trace-content-v1", "operation_facts": true, "source_pages": true}, result["data"])
}

func TestReadModelCapabilitiesRejectQueriesAndWrites(t *testing.T) {
	for _, query := range []string{"?space_id=space", "?include_facts=true", "?x=1&x=2"} {
		require.Equal(t, http.StatusBadRequest, runCapabilitiesRequest(http.MethodGet, query).Code)
	}
	require.Equal(t, http.StatusMethodNotAllowed, runCapabilitiesRequest(http.MethodPost, "").Code)
}
