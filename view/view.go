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

	index := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contents, err := Static.ReadFile("dist/index.html")
		if err != nil {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(contents)
	})
	registerRoutes(mux, config.Setting.ClokiReader.ViewPath, index, fileServer)
}

// registerRoutes mounts the UI pages and static files under viewPath.
//
// Every pattern is GET (which also serves HEAD). A pattern without a method
// overlaps the writer's "POST /{target}/_doc" whenever viewPath is a single
// segment such as "/ui", neither pattern is more specific than the other, and
// ServeMux panics at startup.
func registerRoutes(mux *http.ServeMux, viewPath string, index, files http.Handler) {
	prefix := "/"
	if viewPath != "/etc/qryn-view" {
		prefix = viewPath
	}

	// Serve static files
	viewPath = strings.TrimSuffix(viewPath, "/")
	for _, path := range []string{
		viewPath + "/{$}",
		viewPath + "/plugins",
		viewPath + "/users",
		viewPath + "/datasources",
		viewPath + "/datasources/{ds}"} {
		mux.Handle("GET "+path, index)
	}
	mux.Handle("GET "+strings.TrimSuffix(prefix, "/")+"/", files)
}
