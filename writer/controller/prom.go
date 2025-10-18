package controller

import (
	"net/http"

	"github.com/metrico/qryn/v4/writer/utils/unmarshal"
)

// swagger:route GET /api/v1/prom/remote/write Data WriteData
//
// Returns data from server in array
//
// ---
//     Consumes:
//     - application/json
//
// 	   Produces:
// 	   - application/json
//
//	   Security:
//	   - JWT
//     - ApiKeyAuth
//
//
// SecurityDefinitions:
// JWT:
//      type: apiKey
//      name: Authorization
//      in: header
// ApiKeyAuth:
//      type: apiKey
//      in: header
//      name: Auth-Token
///
//  Responses:
//    201: body:TableUserList
//    400: body:FailureResponse

func WriteStreamV2(cfg MiddlewareConfig) func(w http.ResponseWriter, r *http.Request) {

	return Build(
		append(cfg.ExtraMiddleware,
			withTSAndSampleService,
			withUnsnappyRequest,
			withSimpleParser("*", Parser(unmarshal.UnmarshallMetricsWriteProtoV2)),
			withOkStatusAndBody(204, nil))...)
}

func WriteStreamProbeV2(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

//var WriteStreamV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withUnsnappyRequest,
//		withSimpleParser("*", Parser(unmarshal.UnmarshallMetricsWriteProtoV2)),
//		withOkStatusAndBody(204, nil))...)
