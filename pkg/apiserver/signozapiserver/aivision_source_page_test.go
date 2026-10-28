package signozapiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SigNoz/signoz/pkg/http/middleware"
	"github.com/SigNoz/signoz/pkg/modules/aivision"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type sourcePageRouteHandler struct{ aivision.Handler }

func (*sourcePageRouteHandler) ReadModel(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// This exercises the public registration boundary. A standalone domain handler
// test cannot detect a missing production route or an accidentally public one.
func TestSourcePageIsRegisteredBehindMainAPIAuthorization(t *testing.T) {
	p := &provider{aivisionHandler: &sourcePageRouteHandler{}, authzMiddleware: &middleware.AuthZ{}}
	router := mux.NewRouter()
	require.NoError(t, p.addAIVisionRoutes(router))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/source-page?space_id=space&trace_id=abc", nil))
	require.Equal(t, http.StatusUnauthorized, w.Code)
	post := httptest.NewRecorder()
	router.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/api/v1/ai-vision/traces/read-model/source-page", nil))
	require.Equal(t, http.StatusMethodNotAllowed, post.Code)
	capabilities := httptest.NewRecorder()
	router.ServeHTTP(capabilities, httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/capabilities", nil))
	require.Equal(t, http.StatusUnauthorized, capabilities.Code)
}
