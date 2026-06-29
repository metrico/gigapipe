// Package router registers the ruler HTTP routes for both the Loki and
// Prometheus rule sets. Alerting endpoints are intentionally absent: gigapipe
// stores but never evaluates alerting rules.
package router

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/ruler/controller"
)

// Route registers all ruler endpoints. lokiCtrl serves the Loki rule set
// (LogQL), promCtrl the Prometheus rule set (PromQL); each is backed by its own
// type-scoped store and manager.
func Route(router *mux.Router, lokiCtrl, promCtrl *controller.Controller) {
	// Loki ruler API. /api/prom/rules is Loki's own Prometheus-compatible
	// ruler API, used interchangeably by Grafana's Loki datasource.
	for _, prefix := range []string{"/loki/api/v1/rules", "/api/prom/rules"} {
		router.HandleFunc(prefix, lokiCtrl.AllRules).Methods("GET")
		router.HandleFunc(prefix+"/{namespace}", lokiCtrl.RulesByNamespace).Methods("GET")
		router.HandleFunc(prefix+"/{namespace}/{group}", lokiCtrl.GetRuleGroup).Methods("GET")
		router.HandleFunc(prefix+"/{namespace}", lokiCtrl.SetRuleGroup).Methods("POST")
		router.HandleFunc(prefix+"/{namespace}", lokiCtrl.DeleteNamespace).Methods("DELETE")
		router.HandleFunc(prefix+"/{namespace}/{group}", lokiCtrl.DeleteRuleGroup).Methods("DELETE")
	}

	// Prometheus ruler API — Grafana's Prometheus datasource uses /api/v1/rules.
	// The bare GET returns recording rules in Prometheus JSON format.
	router.HandleFunc("/api/v1/rules", promCtrl.PrometheusRules).Methods("GET")
	router.HandleFunc("/api/v1/rules/{namespace}", promCtrl.RulesByNamespace).Methods("GET")
	router.HandleFunc("/api/v1/rules/{namespace}/{group}", promCtrl.GetRuleGroup).Methods("GET")
	router.HandleFunc("/api/v1/rules/{namespace}", promCtrl.SetRuleGroup).Methods("POST")
	router.HandleFunc("/api/v1/rules/{namespace}", promCtrl.DeleteNamespace).Methods("DELETE")
	router.HandleFunc("/api/v1/rules/{namespace}/{group}", promCtrl.DeleteRuleGroup).Methods("DELETE")

	// Debug endpoint — Loki recording rules in Prometheus wire format.
	router.HandleFunc("/prometheus/api/v1/rules", lokiCtrl.PrometheusRules).Methods("GET")
}
