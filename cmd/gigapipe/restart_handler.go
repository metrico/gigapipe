package main

import (
	"github.com/gorilla/mux"
	"github.com/metrico/qryn/v4/reader"
	"github.com/metrico/qryn/v4/writer"
	"net/http"
)

func handleStop(app *mux.Router) {
	app.HandleFunc("/restart", func(w http.ResponseWriter, request *http.Request) {
		writer.Stop()
		reader.Stop()
		stop()
		go start()
		w.Write([]byte("OK"))
	})
}
