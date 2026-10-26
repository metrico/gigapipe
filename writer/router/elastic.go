package apirouterv1

import (
	"net/http"

	controllerv1 "github.com/metrico/qryn/v5/writer/controller"
)

func RouteElasticDataApis(router *http.ServeMux, cfg controllerv1.MiddlewareConfig) {
	router.HandleFunc("POST /{target}/_doc", controllerv1.TargetDocV2(cfg))
	router.HandleFunc("POST /{target}/_create/{id}", controllerv1.TargetDocV2(cfg))
	router.HandleFunc("PUT /{target}/_doc/{id}", controllerv1.TargetDocV2(cfg))
	router.HandleFunc("PUT /{target}/_create/{id}", controllerv1.TargetDocV2(cfg))
	router.HandleFunc("POST /_bulk", controllerv1.TargetBulkV2(cfg))
	router.HandleFunc("POST /{target}/_bulk", controllerv1.TargetBulkV2(cfg))
}
