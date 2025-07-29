package validator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// FlexibleTime is a custom type that can parse time from either
// an integer timestamp or an RFC3339 formatted string
type FlexibleTime time.Time

// UnmarshalText implements the encoding.TextUnmarshaler interface
// allowing FlexibleTime to be used with schema.Decoder
func (ft *FlexibleTime) UnmarshalText(text []byte) error {
	str := string(text)
	// Try parsing as integer timestamp first
	timestamp, err := strconv.ParseInt(str, 10, 64)
	if err == nil {
		*ft = FlexibleTime(time.Unix(timestamp, 0))
		return nil
	}
	// If not an integer, try as RFC3339 string
	t, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return fmt.Errorf("in FlexibleTime.UnmarshalText: failed to parse time parameter: must be either a unix timestamp or RFC3339 formatted string")
	}
	*ft = FlexibleTime(t)
	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
// allowing FlexibleTime to be used with json.Decoder
func (ft *FlexibleTime) UnmarshalJSON(data []byte) error {
	// Handle null value
	if string(data) == "null" {
		return nil // Keep the zero value
	}
	// Try parsing as a JSON number (timestamp)
	var timestamp int64
	err := json.Unmarshal(data, &timestamp)
	if err == nil {
		*ft = FlexibleTime(time.Unix(timestamp, 0))
		return nil
	}
	// Try parsing as a JSON string
	var timeStr string
	err = json.Unmarshal(data, &timeStr)
	if err != nil {
		return fmt.Errorf("in FlexibleTime.UnmarshalJSON: failed to parse time parameter: %w", err)
	}
	// Parse the time string
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		return fmt.Errorf("in FlexibleTime.UnmarshalJSON: failed to parse time string: %w", err)
	}
	*ft = FlexibleTime(t)
	return nil
}

// MarshalJSON implements the json.Marshaler interface
// to properly convert FlexibleTime to JSON
func (ft FlexibleTime) MarshalJSON() ([]byte, error) {
	// Convert to RFC3339 string
	return json.Marshal(ft.String())
}

// Time converts FlexibleTime back to time.Time
func (ft FlexibleTime) Time() time.Time {
	return time.Time(ft)
}

// IsZero reports whether FlexibleTime represents the zero time instant, January 1, year 1, 00:00:00 UTC.
func (ft FlexibleTime) IsZero() bool {
	return time.Time(ft).IsZero()
}

// String returns the RFC3339 representation of the time
func (ft FlexibleTime) String() string {
	return time.Time(ft).Format(time.RFC3339)
}

// SetDefaultIfZero sets the FlexibleTime value to a default if it is zero.
// The default string can be "now", "now-<duration>", a unix timestamp, or an RFC3339 time.
func (ft *FlexibleTime) SetDefaultIfZero(defaultStr string) {
	if ft.IsZero() && defaultStr != "" {
		// Handle common default cases (now, now-6h, RFC3339, unix)
		now := time.Now()
		switch {
		case defaultStr == "now":
			*ft = FlexibleTime(now)
		case strings.HasPrefix(defaultStr, "now-"):
			dur, err := time.ParseDuration("-" + strings.TrimPrefix(defaultStr, "now-"))
			if err == nil {
				*ft = FlexibleTime(now.Add(dur))
			}
		default:
			_ = ft.UnmarshalText([]byte(defaultStr))
		}
	}
}

// applyFlexibleTimeDefaults applies default values to FlexibleTime fields in a struct pointer.
// It looks for struct tags named "flexdefault" and sets zero FlexibleTime fields to the specified default.
func applyFlexibleTimeDefaults(ptr any) {
	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()
	for i := range t.NumField() {
		field := t.Field(i)
		def := field.Tag.Get("flexdefault")
		fv := v.Field(i)
		if fv.CanAddr() && fv.Type() == reflect.TypeOf(FlexibleTime{}) {
			ft := fv.Addr().Interface().(*FlexibleTime)
			ft.SetDefaultIfZero(def)
		}
	}
}
