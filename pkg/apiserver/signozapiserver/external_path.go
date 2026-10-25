package signozapiserver

import "net/http"

// withExternalPath keeps probes reachable at their local root URLs while
// ordinary API routes remain behind the configured reverse-proxy prefix.
func withExternalPath(router, handler http.Handler, prefix string) http.Handler {
	if prefix == "" {
		return handler
	}
	prefixed := http.StripPrefix(prefix, handler)
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/api/v1/health", "/api/v1/ready", "/api/v2/healthz", "/api/v2/readyz", "/api/v2/livez":
			router.ServeHTTP(w, req)
			return
		}
		prefixed.ServeHTTP(w, req)
	})
}
