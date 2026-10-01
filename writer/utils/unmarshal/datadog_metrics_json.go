package unmarshal

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v5/writer/utils/errors"
)

type datadogMetricsRequestDec struct {
	ctx *ParserCtx

	Labels [][]string
	tsMs   []int64
	values []float64

	path []any

	onMetricSamples  onMetricSamplesHandler
	onMetricMetadata onMetricMetadataHandler
}

func (d *datadogMetricsRequestDec) Decode() error {
	dec := jx.Decode(d.ctx.bodyReader, 64*1024)
	return d.WrapError(dec.Obj(func(dec *jx.Decoder, key string) error {
		switch key {
		case "series":
			d.path = append(d.path, "series")
			return d.WrapError(dec.Arr(func(dec *jx.Decoder) error {
				d.Labels = d.Labels[:0]
				d.tsMs = d.tsMs[:0]
				d.values = d.values[:0]
				err := d.WrapError(dec.Obj(d.DecodeSeriesItem))
				if err != nil {
					return err
				}
				return d.WrapError(d.onMetricSamples(d.Labels, d.tsMs, d.values, nil))
			}))
		}
		return d.WrapError(dec.Skip())
	}))
}

func (d *datadogMetricsRequestDec) SetOnMetricSamples(h onMetricSamplesHandler) {
	d.onMetricSamples = h
}

func (d *datadogMetricsRequestDec) SetOnMetricMetadata(h onMetricMetadataHandler) {
	d.onMetricMetadata = h
}

func (d *datadogMetricsRequestDec) DecodeSeriesItem(dec *jx.Decoder, key string) error {
	switch key {
	case "metric":
		d.path = append(d.path, "series")
		val, err := d.MaybeString(dec)
		d.Labels = append(d.Labels, []string{"__name__", val})
		d.path = d.path[:len(d.path)-1]
		return d.WrapError(err)
	case "resources":
		d.path = append(d.path, "resources")
		i := -1
		d.path = append(d.path, &i)
		err := d.WrapError(d.MaybeArr(dec, func(dec *jx.Decoder) error {
			i++
			return d.WrapError(d.MaybeObj(dec, func(dec *jx.Decoder, key string) error {
				d.path = append(d.path, key)
				val, err := d.MaybeString(dec)
				d.Labels = append(d.Labels, []string{fmt.Sprintf("resource%d_%s", i+1, key), val})
				d.path = d.path[:len(d.path)-1]
				return d.WrapError(err)
			}))
		}))
		d.path = d.path[:len(d.path)-2]
		return d.WrapError(err)
	case "points":
		d.path = append(d.path, "points")
		tsMs := time.Now().UnixMilli()
		val := float64(0)
		i := -1
		d.path = append(d.path, &i)
		err := d.WrapError(dec.Arr(func(dec *jx.Decoder) error {
			i++
			err := d.WrapError(dec.Obj(func(dec *jx.Decoder, key string) error {
				var err error
				switch key {
				case "timestamp":
					d.path = append(d.path, "timestamp")
					tsMs, err = dec.Int64()
					err = d.WrapError(err)
					tsMs *= 1000
					d.path = d.path[:len(d.path)-1]
					return d.WrapError(err)
				case "value":
					d.path = append(d.path, "timestamp")
					val, err = dec.Float64()
					err = d.WrapError(err)
					d.path = d.path[:len(d.path)-1]
					return d.WrapError(err)
				}
				return d.WrapError(dec.Skip())
			}))
			d.tsMs = append(d.tsMs, tsMs)
			d.values = append(d.values, val)
			return d.WrapError(err)
		}))
		d.path = d.path[:len(d.path)-2]
		return d.WrapError(err)
	}
	return d.WrapError(dec.Skip())
}

func (d *datadogMetricsRequestDec) MaybeString(dec *jx.Decoder) (string, error) {
	tp := dec.Next()
	switch tp {
	case jx.String:
		res, err := dec.Str()
		return res, d.WrapError(err)
	}
	return "", nil
}
func (d *datadogMetricsRequestDec) MaybeObj(dec *jx.Decoder, f func(d *jx.Decoder, key string) error) error {
	tp := dec.Next()
	switch tp {
	case jx.Object:
		return d.WrapError(dec.Obj(f))
	}
	return nil
}

func (d *datadogMetricsRequestDec) MaybeArr(dec *jx.Decoder, f func(d *jx.Decoder) error) error {
	tp := dec.Next()
	switch tp {
	case jx.Array:
		return d.WrapError(dec.Arr(f))
	}
	return nil
}

func (d *datadogMetricsRequestDec) WrapError(err error) error {
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "json error") {
		return errors.NewUnmarshalError(err)
		//return err
	}
	var path strings.Builder
	for _, i := range d.path {
		switch i := i.(type) {
		case string:
			path.WriteString(".")
			path.WriteString(i)
		case *int:
			fmt.Fprintf(&path, ".%d", *i)
		}
	}

	return errors.NewUnmarshalError(fmt.Errorf("json error path: %s; error: %s", path.String(), err.Error()))
}

var UnmarshallDatadogMetricsV2JSONV2 = Build(
	withMetricsParser(func(ctx *ParserCtx) iMetricsParser {
		return &datadogMetricsRequestDec{ctx: ctx}
	}))
