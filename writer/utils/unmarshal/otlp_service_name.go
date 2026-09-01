package unmarshal

// otlpServiceNameKeys lists the OTLP attributes that can name the service a
// signal came from, in descending order of priority: the first key that
// resolves to a non-empty value wins.
//
// There is deliberately ONE list for every OTLP signal. Traces resolve a span's
// service from it (otlpGetServiceNames) and profiles resolve a profile's
// service from it (extractOTLPMeta). Keeping a copy per signal would let them
// drift, and the same process would then land under two different service names
// depending on which signal it emitted — which breaks exactly the correlation
// (this trace, and the profile of the process that served it) the shared name
// exists to provide.
//
// Add a key here, not in a caller.
var otlpServiceNameKeys = []string{
	"service.name",
	"faas.name",
	"k8s.deployment.name",
	"process.executable.name",
}

// otlpLocalServiceNameKeys is otlpServiceNameKeys with peer.service ahead of
// it. It applies only when resolving the LOCAL service of a span, where an
// explicit peer.service names the service the span is talking to and therefore
// outranks the resource's own identity.
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
