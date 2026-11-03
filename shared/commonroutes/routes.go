package commonroutes

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterCommonRoutes registers the common routes to the given mux.
func RegisterCommonRoutes(app *http.ServeMux) {
	app.HandleFunc("GET /ready", Ready)
	app.HandleFunc("GET /config", Config)
	app.Handle("GET /metrics", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			DisableCompression: true,
		}),
	))
	app.HandleFunc("GET /api/status/buildinfo", BuildInfo)
}
