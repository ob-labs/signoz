package signozapiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadinessIgnoresExternalPathForLocalProbe(t *testing.T) {
	for _, prefix := range []string{"", "/signoz"} {
		t.Run(prefix, func(t *testing.T) {
			router := http.NewServeMux()
			router.HandleFunc("/api/v1/ready", func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					t.Fatal("local readiness should not require credentials")
				}
				_, _ = w.Write([]byte("ready"))
			})
			handler := withExternalPath(router, router, prefix)
			for _, path := range []string{"/api/v1/ready", prefix + "/api/v1/ready"} {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				if response.Code != http.StatusOK || response.Body.String() != "ready" {
					t.Fatalf("anonymous %s with prefix %q = %d %s", path, prefix, response.Code, response.Body.String())
				}
			}
		})
	}
}

func TestExternalPathPreservesOtherProbeAndApplicationRoutes(t *testing.T) {
	router := http.NewServeMux()
	paths := []string{"/api/v1/health", "/api/v2/healthz", "/api/v2/readyz", "/api/v2/livez", "/api/v1/application"}
	for _, path := range paths {
		router.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(r.URL.Path)) })
	}
	handler := withExternalPath(router, router, "/signoz")
	for _, path := range paths {
		for _, prefix := range []string{"", "/signoz"} {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, prefix+path, nil))
			want := http.StatusOK
			if prefix == "" && path == "/api/v1/application" {
				want = http.StatusNotFound
			}
			if response.Code != want {
				t.Errorf("%s = %d; want %d", prefix+path, response.Code, want)
			}
			if want == http.StatusOK && response.Body.String() != path {
				t.Errorf("%s reached wrong route: %s", prefix+path, response.Body.String())
			}
		}
	}
}
