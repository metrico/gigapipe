package model

import "github.com/metrico/qryn/writer/utils/helpers"

type ParserResponse struct {
	Error             error
	TimeSeriesRequest helpers.SizeGetter
	SamplesRequest    helpers.SizeGetter
	SpansAttrsRequest helpers.SizeGetter
	SpansRequest      helpers.SizeGetter
	ProfileRequest    helpers.SizeGetter
}
