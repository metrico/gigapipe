package apirouterv1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

// TestProfileRoutesClaimEveryPushPath pins the paths themselves. The handlers
// need live insert services, so this matches rather than serves: what can
// silently regress here is the spelling of a route, and a misspelled
// /push.v1.PusherService/Push is indistinguishable from no support at all --
// an SDK gets a 404 on a path it does not name in its own error output.
func TestProfileRoutesClaimEveryPushPath(t *testing.T) {
	router := mux.NewRouter()
	RouteProfileDataApis(router, controllerv1.MiddlewareConfig{})

	for _, path := range []string{
		"/ingest",                     // legacy pprof upload
		"/v1development/profiles",     // OTLP
		"/push.v1.PusherService/Push", // Pyroscope SDKs >= 1.0, over Connect
	} {
		var match mux.RouteMatch
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(""))
		if !router.Match(req, &match) {
			t.Errorf("POST %s is not routed", path)
		}
	}
}
