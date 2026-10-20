package cmd

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func executeHealthcheck(args ...string) error {
	RootCmd.SetArgs(append([]string{"healthcheck"}, args...))
	RootCmd.SetOut(io.Discard)
	RootCmd.SetErr(io.Discard)
	return RootCmd.Execute()
}

func serverPort(t *testing.T, server *httptest.Server) string {
	t.Helper()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestHealthcheckHTTPStatus(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNoContent, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.RequestURI() != "/api/v1/health" {
					t.Errorf("unexpected health request: %s %s", r.Method, r.URL.RequestURI())
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			err := executeHealthcheck("--port", serverPort(t, server))
			if (err == nil) != (status == http.StatusOK) {
				t.Fatalf("status %d: got error %v", status, err)
			}
		})
	}
}

func TestHealthcheckRejectsRedirect(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	if err := executeHealthcheck("--port", serverPort(t, server)); err == nil {
		t.Fatal("redirect unexpectedly reported healthy")
	}
	if got := targetHits.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests", got)
	}
}

func TestHealthcheckConnectionRefused(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	listener.Close()
	if err := executeHealthcheck("--port", port); err == nil {
		t.Fatal("closed listener unexpectedly reported healthy")
	}
}

func TestHealthcheckTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	start := time.Now()
	err := executeHealthcheck("--port", serverPort(t, server))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("unresponsive server unexpectedly reported healthy")
	}
	if elapsed < 1800*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("request took %v, expected a two-second deadline", elapsed)
	}
}

func TestHealthcheckRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"unexpected"}, {"--unknown"}, {"--port", "0"}, {"--port", "-1"}, {"--port", "65536"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			if err := executeHealthcheck(args...); err == nil {
				t.Fatal("invalid arguments unexpectedly succeeded")
			}
		})
	}
}
