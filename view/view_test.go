package view

import (
	"net/http"
	"net/http/httptest"
	"testing"

	clokiconfig "github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/shared/commonroutes"
	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
	"github.com/metrico/qryn/v5/writer/plugin"
)

// sharedMux builds the mux that view shares in mode "all": the common routes
// and the writer routes, whose "/{target}/..." patterns are the ones a UI
// pattern can conflict with.
func sharedMux() *http.ServeMux {
	mux := http.NewServeMux()
	commonroutes.RegisterCommonRoutes(mux)
	(&plugin.QrynWriterPlugin{}).RegisterRoutes(clokiconfig.ClokiBaseSettingServer{},
		controllerv1.MiddlewareConfig{}, controllerv1.MiddlewareConfig{}, mux)
	return mux
}

var viewPaths = []string{"/etc/qryn-view", "", "/", "/ui", "/ui/", "/a/b", "/_bulk"}

func TestRegisterRoutesDoesNotConflict(t *testing.T) {
	for _, vp := range viewPaths {
		t.Run(vp, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("view_path %q: registering the UI routes panicked: %v", vp, r)
				}
			}()
			registerRoutes(sharedMux(), vp, http.NotFoundHandler(), http.NotFoundHandler())
		})
	}
}

// The UI must not take requests the writer serves, and must serve its own pages.
func TestRegisterRoutesPrecedence(t *testing.T) {
	mux := sharedMux()
	registerRoutes(mux, "/ui", http.NotFoundHandler(), http.NotFoundHandler())
	for _, c := range []struct{ method, path, want string }{
		{http.MethodPost, "/ui/_doc", "POST /{target}/_doc"},
		{http.MethodPost, "/_bulk", "POST /_bulk"},
		{http.MethodGet, "/ready", "GET /ready"},
		{http.MethodGet, "/ui/", "GET /ui/{$}"},
		{http.MethodHead, "/ui/plugins", "GET /ui/plugins"},
		{http.MethodGet, "/ui/datasources/x", "GET /ui/datasources/{ds}"},
		{http.MethodGet, "/ui/assets/app.js", "GET /ui/"},
	} {
		_, got := mux.Handler(httptest.NewRequest(c.method, c.path, nil))
		if got != c.want {
			t.Errorf("%s %s matched %q, want %q", c.method, c.path, got, c.want)
		}
	}
}
