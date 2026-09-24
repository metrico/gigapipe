package view

import (
	"io/fs"
	"net/http"
	"strings"

	clconfig "github.com/metrico/cloki-config"
)

var config *clconfig.ClokiConfig

func Init(cfg *clconfig.ClokiConfig, mux *http.ServeMux) {
	if !HaveStatic {
		return
	}

	config = cfg

	staticSub, err := fs.Sub(Static, "dist")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(staticSub))

	prefix := "/"
	if config.Setting.ClokiReader.ViewPath != "/etc/qryn-view" {
		prefix = config.Setting.ClokiReader.ViewPath
	}

	// Serve static files
	viewPath := strings.TrimSuffix(config.Setting.ClokiReader.ViewPath, "/")
	for _, path := range []string{
		viewPath + "/{$}",
		viewPath + "/plugins",
		viewPath + "/users",
		viewPath + "/datasources",
		viewPath + "/datasources/{ds}"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			contents, err := Static.ReadFile("dist/index.html")
			if err != nil {
				w.WriteHeader(404)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.Write(contents)
		})
	}
	mux.Handle(strings.TrimSuffix(prefix, "/")+"/", fileServer)
}
