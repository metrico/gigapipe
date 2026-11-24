package unmarshal

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	pprof_proto "github.com/google/pprof/profile"
	"github.com/metrico/qryn/v5/writer/model"
)

const (
	// pushV1NameLabel is Pyroscope's original spelling of the application name.
	pushV1NameLabel = "__name__"
	// pushV1ServiceLabel is the OpenTelemetry-aligned spelling that current SDK
	// versions and Grafana Alloy populate.
	pushV1ServiceLabel = "service_name"
)

// pushV1ProfProtoDec ingests a Pyroscope v2 Connect push.
//
// The legacy /ingest upload takes one profile and reads its application name,
// tags and time window from query parameters. A v2 push instead carries many
// profiles in one body, grouped into series that each describe themselves with
// labels, and carries no time window at all. This decoder translates the one
// shape into the other and then reuses the same conversion as every other
// profile path.
type pushV1ProfProtoDec struct {
	pProfProtoDec
}

func (p *pushV1ProfProtoDec) Decode() error {
	body, err := io.ReadAll(p.ctx.bodyReader)
	if err != nil {
		return fmt.Errorf("failed to read push request: %w", err)
	}

	req, err := parsePushV1Request(body)
	if err != nil {
		return err
	}

	for _, series := range req.Series {
		name, tags := pushV1NameAndTags(series.Labels)
		if name == "" {
			// Without a name there is nothing to store the series under, and
			// guessing one would silently scatter profiles across made-up
			// applications. Say so instead.
			return fmt.Errorf("push.v1: series has neither a %s nor a %s label",
				pushV1ServiceLabel, pushV1NameLabel)
		}

		for _, sample := range series.Samples {
			if len(sample.RawProfile) == 0 {
				continue
			}
			profiles, err := Parse(bytes.NewBuffer(sample.RawProfile))
			if err != nil {
				return fmt.Errorf("failed to parse pprof for %q: %w", name, err)
			}
			for _, profile := range profiles {
				timestampNs, durationNs := pushV1Window(profile.Profile)
				if err := emitProfile(p.onProfiles, profile,
					timestampNs, durationNs, name, tags); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// pushV1Window reads the time window out of the pprof payload.
//
// A v2 push has no equivalent of /ingest's from and until parameters, so the
// window can only come from the profile itself, where the SDKs set both
// fields. The timestamp falls back to now because a profile has to land
// somewhere on the time axis to ever be queried again; the duration does not
// fall back, because every value that could be invented here -- the SDK's
// upload interval, the profile's sampling period -- would be a guess stored as
// though it were measured.
func pushV1Window(profile *pprof_proto.Profile) (timestampNs, durationNs uint64) {
	timestampNs = uint64(time.Now().UnixNano())
	if profile == nil {
		return timestampNs, 0
	}
	if profile.TimeNanos > 0 {
		timestampNs = uint64(profile.TimeNanos)
	}
	if profile.DurationNanos > 0 {
		durationNs = uint64(profile.DurationNanos)
	}
	return timestampNs, durationNs
}

// pushV1NameAndTags splits a series' labels the way /ingest splits its
// `name=app{key=value}` parameter: one application name, everything else a tag.
//
// service_name wins over __name__ when a client sends both, following the
// OpenTelemetry-aligned convention the current SDKs moved to. Labels wrapped
// in double underscores (__session_id__, __delta__, and whatever a later
// release adds) are Pyroscope-internal and are not user tags.
//
// service_name is left out of the tags because the profiles_series
// materialized view appends it there itself, along with the fingerprint it
// takes part in (ctrl/qryn/sql/profiles.sql).
//
// Tags come back sorted by name so the stored tags column reads the same way
// for the same series whatever order a client sent its labels in. This is
// cosmetic rather than load-bearing: that same view fingerprints over
// arraySort(tags), so series identity already survives any label order.
func pushV1NameAndTags(labels []pushV1LabelPair) (string, []model.StrStr) {
	var name, legacyName string
	var tags []model.StrStr

	for _, label := range labels {
		switch {
		case label.Name == pushV1ServiceLabel:
			name = label.Value
		case label.Name == pushV1NameLabel:
			legacyName = label.Value
		case strings.HasPrefix(label.Name, "__") && strings.HasSuffix(label.Name, "__"):
			// Pyroscope-internal, not a tag the user set.
		default:
			tags = append(tags, model.StrStr{Str1: label.Name, Str2: label.Value})
		}
	}

	if name == "" {
		name = legacyName
	}
	slices.SortFunc(tags, func(a, b model.StrStr) int {
		return strings.Compare(a.Str1, b.Str1)
	})
	return name, tags
}

// UnmarshalPushV1ProfileProtoV2 ingests a Pyroscope v2 Connect push. Unlike
// the /ingest unmarshallers it reads no context values: a push request
// describes its own series and carries its own timestamps.
var UnmarshalPushV1ProfileProtoV2 = Build(
	withProfileParser(func(ctx *ParserCtx) iProfilesParser {
		return &pushV1ProfProtoDec{pProfProtoDec{ctx: ctx}}
	}))
