package controller

import (
	"net/http"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v5/reader/utils/logger"
	watchdog "github.com/metrico/qryn/v5/reader/watchdog"
)

type MiscController struct {
	Version string
}

func (uc *MiscController) Ready(w http.ResponseWriter, r *http.Request) {
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

func (uc *MiscController) Config(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Not supported"))
}

func (uc *MiscController) Rules(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"data": {"groups": []},"status": "success"}`))
}

func (uc *MiscController) Metadata(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "success","data": {}}`))
}

func (uc *MiscController) Buildinfo(w http.ResponseWriter, r *http.Request) {
	//w.Header().Set("Content-Type", "application/json")
	//w.WriteHeader(http.StatusOK)
	//w.Write([]byte(fmt.Sprintf(`{"status": "success","data": {"version": "%s"}}`, uc.Version)))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	stream := &jx.Writer{}
	stream.ObjStart()
	stream.FieldStart("status")
	stream.Str("success")
	stream.Comma()

	stream.FieldStart("data")
	stream.ObjStart()

	stream.FieldStart("version")
	stream.Str(uc.Version)

	stream.ObjEnd()
	stream.ObjEnd()

	w.Write(stream.Buf)

}
