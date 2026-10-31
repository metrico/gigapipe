package unmarshal

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// Decoder for the Pyroscope v2 push request body.
//
// This is the request every Pyroscope SDK from 1.0 onwards sends to
// /push.v1.PusherService/Push over the Connect protocol, whose unary bodies
// are the bare protobuf message with no extra framing:
//
//	message PushRequest { repeated RawProfileSeries series = 1; }
//	message RawProfileSeries {
//	  repeated types.v1.LabelPair labels = 1;
//	  repeated RawSample samples = 2;
//	  repeated types.v1.ProfileAnnotation annotations = 3;
//	}
//	message RawSample { bytes raw_profile = 1; string ID = 2; }
//	message LabelPair { string name = 1; string value = 2; }
//
// It is decoded field by field with protowire instead of from generated code
// because there is no protoc in this repo's build path, and depending on the
// Pyroscope module for four messages would pull its server tree in with it.
// protowire already ships inside google.golang.org/protobuf, and its Consume*
// helpers report malformed input as a negative length rather than panicking --
// which is the property that matters on a handler fed unauthenticated bytes.
//
// Field 3 of RawProfileSeries, annotations, is skipped: gigapipe has nowhere
// to put it, and the generic field walk below drops it the same way it drops
// anything a later SDK release adds.

type pushV1LabelPair struct {
	Name  string
	Value string
}

type pushV1Sample struct {
	// RawProfile is a pprof payload, usually gzipped. It is handed to the
	// pprof parser as-is: that parser sniffs the gzip magic bytes itself
	// (github.com/google/pprof/profile.ParseData), so both forms work.
	RawProfile []byte
	ID         string
}

type pushV1Series struct {
	Labels  []pushV1LabelPair
	Samples []pushV1Sample
}

type pushV1Request struct {
	Series []pushV1Series
}

// walkFields hands every protobuf field in b to fn, which returns the number
// of bytes it consumed, or 0 to say the field is not one it cares about.
//
// Returning 0 is unambiguous: every wire type consumes at least one byte, so
// no field fn actually handles can report a length of zero. Fields fn declines
// are skipped wholesale, which is what keeps this decoder working against SDKs
// that send fields added to the schema after this was written.
func walkFields(b []byte, fn func(num protowire.Number, typ protowire.Type, b []byte) (int, error)) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("push.v1: malformed field tag: %w", protowire.ParseError(n))
		}
		b = b[n:]

		consumed, err := fn(num, typ, b)
		if err != nil {
			return err
		}
		if consumed == 0 {
			if consumed = protowire.ConsumeFieldValue(num, typ, b); consumed < 0 {
				return fmt.Errorf("push.v1: malformed field %d: %w",
					num, protowire.ParseError(consumed))
			}
		}
		b = b[consumed:]
	}
	return nil
}

// bytesField consumes a length-delimited field and passes its contents to fn.
func bytesField(b []byte, what string, fn func([]byte) error) (int, error) {
	v, n := protowire.ConsumeBytes(b)
	if n < 0 {
		return 0, fmt.Errorf("push.v1: malformed %s: %w", what, protowire.ParseError(n))
	}
	if err := fn(v); err != nil {
		return 0, err
	}
	return n, nil
}

func parsePushV1LabelPair(b []byte) (pushV1LabelPair, error) {
	var out pushV1LabelPair
	err := walkFields(b, func(num protowire.Number, typ protowire.Type, b []byte) (int, error) {
		if typ != protowire.BytesType || (num != 1 && num != 2) {
			return 0, nil
		}
		return bytesField(b, "label pair", func(v []byte) error {
			if num == 1 {
				out.Name = string(v)
			} else {
				out.Value = string(v)
			}
			return nil
		})
	})
	return out, err
}

func parsePushV1Sample(b []byte) (pushV1Sample, error) {
	var out pushV1Sample
	err := walkFields(b, func(num protowire.Number, typ protowire.Type, b []byte) (int, error) {
		if typ != protowire.BytesType || (num != 1 && num != 2) {
			return 0, nil
		}
		return bytesField(b, "raw sample", func(v []byte) error {
			if num == 1 {
				// Copied, not aliased: the caller's buffer is pooled and the
				// payload outlives this call by way of the pprof parser.
				out.RawProfile = append([]byte(nil), v...)
			} else {
				out.ID = string(v)
			}
			return nil
		})
	})
	return out, err
}

func parsePushV1Series(b []byte) (pushV1Series, error) {
	var out pushV1Series
	err := walkFields(b, func(num protowire.Number, typ protowire.Type, b []byte) (int, error) {
		if typ != protowire.BytesType || (num != 1 && num != 2) {
			return 0, nil
		}
		return bytesField(b, "profile series", func(v []byte) error {
			if num == 1 {
				label, err := parsePushV1LabelPair(v)
				if err != nil {
					return err
				}
				out.Labels = append(out.Labels, label)
				return nil
			}
			sample, err := parsePushV1Sample(v)
			if err != nil {
				return err
			}
			out.Samples = append(out.Samples, sample)
			return nil
		})
	})
	return out, err
}

// parsePushV1Request decodes a push.v1 PushRequest.
func parsePushV1Request(b []byte) (pushV1Request, error) {
	var out pushV1Request
	err := walkFields(b, func(num protowire.Number, typ protowire.Type, b []byte) (int, error) {
		if num != 1 || typ != protowire.BytesType {
			return 0, nil
		}
		return bytesField(b, "push request", func(v []byte) error {
			series, err := parsePushV1Series(v)
			if err != nil {
				return err
			}
			out.Series = append(out.Series, series)
			return nil
		})
	})
	return out, err
}
