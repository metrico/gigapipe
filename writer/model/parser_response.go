package model

import "github.com/metrico/qryn/v4/writer/utils/helpers"

type ParserResponse struct {
	Error             error
	TimeSeriesRequest helpers.SizeGetter
	SamplesRequest    helpers.SizeGetter
	SpansAttrsRequest helpers.SizeGetter
	SpansRequest      helpers.SizeGetter
	ProfileRequest    helpers.SizeGetter
	MetadataRequest   helpers.SizeGetter
}
