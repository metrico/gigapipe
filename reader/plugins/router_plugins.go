package plugins

import (
	"net/http"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/prometheus/prometheus/storage"
)

type Services struct {
	TempoService       model.ITempoService
	QueryLabelsService model.IQueryLabelsService
	PrometheusService  storage.Queryable
	QueryRangeService  model.IQueryRangeService
	ServiceData        model.ServiceData
}

// IRoutePlugin mounts extra HTTP routes on the shared mux. Patterns use
// http.ServeMux syntax ("GET /path/{name}") and handlers read path parameters
// with r.PathValue; registering a pattern that duplicates or conflicts with an
// existing one panics at startup.
type IRoutePlugin interface {
	Route(router *http.ServeMux)
	SetServices(services Services)
}

var routePlugins []IRoutePlugin

func RegisterRoutePlugin(name string, p IRoutePlugin) {
	routePlugins = append(routePlugins, p)
}

func GetRoutePlugins() []IRoutePlugin {
	return routePlugins
}
