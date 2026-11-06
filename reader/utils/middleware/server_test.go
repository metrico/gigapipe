package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/cloki-config/config"
)

func serverConfig(cors, auth bool) *clconfig.ClokiConfig {
	setting := &config.ClokiBaseSettingServer{}
	setting.HTTP_SETTINGS.Cors.Enable = cors
	if auth {
		setting.AUTH_SETTINGS.BASIC.Username = testLogin
		setting.AUTH_SETTINGS.BASIC.Password = testPass
	}
	return &clconfig.ClokiConfig{Setting: setting}
}

func serveWrapped(cfg *clconfig.ClokiConfig, h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	WrapServer(cfg, h).ServeHTTP(w, req)
	return w
}

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
})

// A browser sends the CORS preflight without credentials, so CORS must answer
// it before basic auth can reject it.
func TestWrapServerPreflightSkipsAuth(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/v1/query", nil)
	w := serveWrapped(serverConfig(true, true), okHandler, req)
	if w.Code != http.StatusOK {
		t.Fatalf("preflight status=%d, want 200", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("preflight response has no Access-Control-Allow-Origin")
	}
}

// A 401 must carry the CORS headers too, or the browser hides it from the page
// that made the request.
func TestWrapServerUnauthorizedCarriesCors(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	w := serveWrapped(serverConfig(true, true), okHandler, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("401 response has no Access-Control-Allow-Origin")
	}
}

func TestWrapServerAuthenticatedGzip(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	req.SetBasicAuth(testLogin, testPass)
	req.Header.Set("Accept-Encoding", "gzip")
	w := serveWrapped(serverConfig(true, true), okHandler, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", w.Code)
	}
	assertGzipStream(t, w, "ok")
}

func TestWrapServerWithoutAuthPassesThrough(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	w := serveWrapped(serverConfig(false, false), okHandler, req)
	if w.Code != http.StatusOK || w.Body.String() != "ok" {
		t.Fatalf("status=%d body=%q, want 200 \"ok\"", w.Code, w.Body.String())
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS header set with CORS disabled")
	}
}
