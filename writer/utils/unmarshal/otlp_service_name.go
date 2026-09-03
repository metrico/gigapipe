package unmarshal

// otlpServiceNameKeys lists the OTLP attributes that can name the service a
// signal came from, in descending order of priority: the first key that
// resolves to a non-empty value wins.
//
// There is ONE list for every OTLP signal this package ingests. Traces resolve a
// span's service from it (otlpGetServiceNames) and profiles resolve a profile's
// service from it (extractOTLPMeta). Keeping a copy per signal would let them
// drift, and the same process would then land under two different service names
// depending on which signal it emitted — which breaks exactly the correlation
// (this trace, and the profile of the process that served it) the shared name
// exists to provide.
//
// The ingest package is where that sharing currently stops:
// reader/service/parse_otlp_json.go inlines its own copy of these keys for
// query-time trace parsing. Unifying the two means lifting the list into
// shared/otlp and reconciling two further differences on the read side (it
// accepts only string-typed attribute values, and falls back to
// "OTLPResourceNoServiceName" rather than "unknown_service"), which changes read
// behaviour and belongs in its own commit.
//
// Within this package: add a key here, not in a caller.
var otlpServiceNameKeys = []string{
	"service.name",
	"faas.name",
	"k8s.deployment.name",
	"process.executable.name",
}

// otlpLocalServiceNameKeys is otlpServiceNameKeys with peer.service ahead of it.
// It applies only when resolving the LOCAL service of a span.
//
// peer.service names the REMOTE service by OTel convention, so its leading the
// LOCAL list is odd. The order is pre-existing: this list reproduces the inline
// list otlpGetServiceNames walked before, and changing it would re-attribute
// spans that are already stored. Fix it, if at all, in a commit where that
// behaviour change is the visible point.
//
// The slice copies otlpServiceNameKeys at init, so it picks up keys added to the
// literal above but not a runtime append to that slice. Nothing appends at
// runtime; if something ever does, build both lists from a common base.
var otlpLocalServiceNameKeys = append([]string{"peer.service"}, otlpServiceNameKeys...)

// resolveOTLPServiceName returns the first non-empty value lookup yields for
// keys, in order, or fallback when none of them resolve.
//
// An attribute that is present but empty is treated as absent rather than as an
// answer: a producer that sets service.name to "" has told us nothing, and
// letting it win would erase a perfectly good process.executable.name further
// down the list.
//
// lookup is supplied by the caller so the same resolution works over any
// attribute representation — pdata's pcommon.Map for profiles, the raw protobuf
// KeyValue slice for traces — without either signal converting its attributes
// into the other's shape first.
func resolveOTLPServiceName(keys []string, fallback string, lookup func(key string) string) string {
	for _, key := range keys {
		if v := lookup(key); v != "" {
			return v
		}
	}
	return fallback
}
