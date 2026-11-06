// Package router registers the ruler HTTP routes for both the Loki and
// Prometheus rule sets. Alerting endpoints are intentionally absent: gigapipe
// stores but never evaluates alerting rules.
package router

import (
	"net/http"

	"github.com/metrico/qryn/v5/ruler/controller"
)

// Route registers all ruler endpoints. lokiCtrl serves the Loki rule set
// (LogQL), promCtrl the Prometheus rule set (PromQL); each is backed by its own
// type-scoped store and manager.
func Route(router *http.ServeMux, lokiCtrl, promCtrl *controller.Controller) {
	// Loki ruler API. /api/prom/rules is Loki's own Prometheus-compatible
	// ruler API, used interchangeably by Grafana's Loki datasource.
	for _, prefix := range []string{"/loki/api/v1/rules", "/api/prom/rules"} {
		router.HandleFunc("GET "+prefix, lokiCtrl.AllRules)
		router.HandleFunc("GET "+prefix+"/{namespace}", lokiCtrl.RulesByNamespace)
		router.HandleFunc("GET "+prefix+"/{namespace}/{group}", lokiCtrl.GetRuleGroup)
		router.HandleFunc("POST "+prefix+"/{namespace}", lokiCtrl.SetRuleGroup)
		router.HandleFunc("DELETE "+prefix+"/{namespace}", lokiCtrl.DeleteNamespace)
		router.HandleFunc("DELETE "+prefix+"/{namespace}/{group}", lokiCtrl.DeleteRuleGroup)
	}

	// Prometheus ruler API — Grafana's Prometheus datasource uses /api/v1/rules.
	// The bare GET returns recording rules in Prometheus JSON format.
	router.HandleFunc("GET /api/v1/rules", promCtrl.PrometheusRules)
	router.HandleFunc("GET /api/v1/rules/{namespace}", promCtrl.RulesByNamespace)
	router.HandleFunc("GET /api/v1/rules/{namespace}/{group}", promCtrl.GetRuleGroup)
	router.HandleFunc("POST /api/v1/rules/{namespace}", promCtrl.SetRuleGroup)
	router.HandleFunc("DELETE /api/v1/rules/{namespace}", promCtrl.DeleteNamespace)
	router.HandleFunc("DELETE /api/v1/rules/{namespace}/{group}", promCtrl.DeleteRuleGroup)

	// Debug endpoint — Loki recording rules in Prometheus wire format.
	router.HandleFunc("GET /prometheus/api/v1/rules", lokiCtrl.PrometheusRules)
}
