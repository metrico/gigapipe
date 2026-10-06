package unmarshal

import (
	"bytes"
	"regexp"
	"slices"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/metrico/qryn/v5/writer/utils"
	"github.com/metrico/qryn/v5/writer/utils/errors"
)

func getMessage(fields map[string]any) (string, error) {
	if len(fields) == 1 {
		return fields["message"].(string), nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, 1000))
	encoder := logfmt.NewEncoder(buf)
	err := encoder.EncodeKeyvals("message", fields["message"])
	if err != nil {
		return "", errors.NewUnmarshalError(err)
	}
	for k, v := range fields {
		if k == "message" {
			continue
		}
		err := encoder.EncodeKeyvals(k, v)
		if err != nil {
			return "", errors.NewUnmarshalError(err)
		}
	}
	return buf.String(), nil
}

// influxDec reads line protocol: a line with a message field is a log entry,
// any other line is one metric sample per numeric field.
type influxDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
	metricSink
}

func (e *influxDec) Decode() error {
	dec := lineprotocol.NewDecoder(e.ctx.bodyReader)
	precision := e.ctx.ctx.Value(utils.ContextKeyPrecision).(lineprotocol.Precision)
	// Lines without a timestamp get the arrival time, truncated to the
	// requested precision.
	now := time.Now()

	for dec.Next() {
		measurement, err := dec.Measurement()
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		labels := [][]string{{"measurement", string(measurement)}}
		for {
			k, v, err := dec.NextTag()
			if err != nil {
				return errors.NewUnmarshalError(err)
			}
			if k == nil {
				break
			}
			key, value := string(k), string(v)
			// A duplicate tag key is invalid line protocol, but the previous
			// parser let the last one win, so keep doing that.
			if i := slices.IndexFunc(labels[1:], func(l []string) bool { return l[0] == key }); i >= 0 {
				labels[i+1][1] = value
				continue
			}
			labels = append(labels, []string{key, value})
		}
		labels = sanitizeLabels(labels)

		fields := map[string]any{}
		for {
			k, v, err := dec.NextField()
			if err != nil {
				return errors.NewUnmarshalError(err)
			}
			if k == nil {
				break
			}
			fields[string(k)] = v.Interface()
		}

		tm, err := dec.Time(precision, now)
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		if _, ok := fields["message"]; ok {
			message, err := getMessage(fields)
			if err != nil {
				return err
			}
			err = e.onEntries(labels, []int64{tm.UnixNano()}, []string{message})
			if err != nil {
				return err
			}
			continue
		}

		for k, v := range fields {
			var fVal float64
			switch v := v.(type) {
			case int64:
				fVal = float64(v)
			case float64:
				fVal = v
			default:
				continue
			}
			series := append(slices.Clone(labels), []string{"__name__", sanitizeMetricName(k)})
			err = e.onMetricSamples(series, []int64{tm.UnixMilli()}, []float64{fVal}, nil)
			if err != nil {
				return err
			}
		}
	}
	if err := dec.Err(); err != nil {
		return errors.NewUnmarshalError(err)
	}
	return nil
}

var metricNameSanitizer = regexp.MustCompile("(^[^a-zA-Z_]|[^a-zA-Z0-9_])")

func sanitizeMetricName(metricName string) string {
	return metricNameSanitizer.ReplaceAllString(metricName, "_")
}

func (e *influxDec) SetOnEntries(h onEntriesHandler) {
	e.onEntries = h
}

var UnmarshalInfluxDBLogsV2 = Build(
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &influxDec{ctx: ctx}
	}))
