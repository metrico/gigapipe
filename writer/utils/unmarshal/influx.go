package unmarshal

import (
	"bytes"
	"regexp"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/errors"
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

type influxDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
}

func (e *influxDec) Decode() error {
	parser := influx.NewStreamParser(e.ctx.bodyReader)
	precision := e.ctx.ctx.Value(utils.ContextKeyPrecision).(time.Duration)
	parser.SetTimePrecision(precision)

	for mtr, err := parser.Next(); true; mtr, err = parser.Next() {
		if err == influx.EOF {
			return nil
		}
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		labels := [][]string{{"measurement", mtr.Name()}}
		for k, v := range mtr.Tags() {
			labels = append(labels, []string{k, v})
		}
		labels = sanitizeLabels(labels)

		fields := mtr.Fields()

		if _, ok := fields["message"]; ok {
			message, err := getMessage(fields)
			if err != nil {
				return err
			}
			err = e.onEntries(labels, []int64{mtr.Time().UnixNano()}, []string{message}, []float64{0},
				[]uint8{model.SAMPLE_TYPE_LOG})
			if err != nil {
				return err
			}
			continue
		}

		labels = append(labels, []string{"__name__", ""})
		nameIdx := len(labels) - 1

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
			labels[nameIdx][1] = sanitizeMetricName(k)
			err = e.onEntries(labels, []int64{mtr.Time().UnixNano()}, []string{""}, []float64{fVal},
				[]uint8{model.SAMPLE_TYPE_METRIC})
			if err != nil {
				return err
			}
		}
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
