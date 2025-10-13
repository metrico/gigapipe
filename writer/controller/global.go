package controller

import (
	"github.com/metrico/qryn/v4/writer/service/registry"
	"github.com/metrico/qryn/v4/writer/utils/numbercache"
)

var Registry registry.ServiceRegistry
var FPCache numbercache.ICache[uint64]
