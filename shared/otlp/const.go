// Package otlp holds constants shared between the writer and reader for the
// OTLP profiles storage contract, so both sides agree on a single source of
// truth instead of duplicating string literals.
package otlp

// ProfilePayloadType is the payload_type value stored on OTLP profile rows
// (profiles/v1development). The writer sets it when persisting the
// self-contained OTLP payload; the reader branches on it in MergeProfiles to
// decode the payload back into a Google-pprof profile.
const ProfilePayloadType = "otel_v1development"
