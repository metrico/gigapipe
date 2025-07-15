package controller

import (
	"github.com/metrico/qryn/writer/service/registry"
	"github.com/metrico/qryn/writer/utils/numbercache"
)

var Registry registry.IServiceRegistry
var FPCache numbercache.ICache[uint64]
