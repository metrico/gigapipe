package commonroutes

import (
	"encoding/json"
	"github.com/metrico/qryn/reader/watchdog"
	"github.com/metrico/qryn/writer/utils/logger"
	"net/http"
)

func Ready(w http.ResponseWriter, r *http.Request) {
	err := watchdog.Check()
	if err != nil {
		w.WriteHeader(500)
		logger.Error(err.Error())
		w.Write([]byte("Internal Server Error"))
		return
	}
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func Config(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Not supported"))
}

func BuildInfo(w http.ResponseWriter, r *http.Request) {
	r.Header.Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"version": "0.0.1", //TODO: Replace with actual version
		"branch":  "main",
	})
}
