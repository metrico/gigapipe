package controller

import (
	"github.com/metrico/qryn/v5/writer/service/registry"
	"github.com/metrico/qryn/v5/writer/utils/metriccache"
	"github.com/metrico/qryn/v5/writer/utils/numbercache"
)

var Registry registry.ServiceRegistry
var FPCache numbercache.ICache[uint64]
var MetricCaches *metriccache.Caches
