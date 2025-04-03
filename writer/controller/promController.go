package controllerv1

import (
	"github.com/metrico/qryn/writer/utils/unmarshal"
	"net/http"
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

//var WriteStreamV2 = Build(
//	append(WithExtraMiddlewareDefault,
//		withTSAndSampleService,
//		withUnsnappyRequest,
//		withSimpleParser("*", Parser(unmarshal.UnmarshallMetricsWriteProtoV2)),
//		withOkStatusAndBody(204, nil))...)
