package unmarshal

import (
	"regexp"
	"time"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils/errors"
)

type datadogRequestDec struct {
	ctx *ParserCtx

	Source     string
	Tags       [][]string
	Hostname   string
	Message    string
	Service    string
	TsMs       int64
	SourceType string

	onEntries onEntriesHandler
}

var tagPattern = regexp.MustCompile(`([\p{L}][\p{L}_0-9\-.\\/]*):([\p{L}_0-9\-.\\/:]+)(,|$)`)

func (d *datadogRequestDec) SetOnEntries(h onEntriesHandler) {
	d.onEntries = h
}

func (d *datadogRequestDec) Decode() error {
	dec := jx.Decode(d.ctx.bodyReader, 64*1024)
	return dec.Arr(func(dec *jx.Decoder) error {
		d.Source = ""
		d.Tags = d.Tags[:0]
		d.Hostname = ""
		d.Message = ""
		d.Service = ""
		d.TsMs = 0
		return d.DecodeEntry(dec)
	})
}

func (d *datadogRequestDec) DecodeEntry(dec *jx.Decoder) error {
	err := dec.Obj(func(dec *jx.Decoder, key string) error {
		var err error
		switch key {
		case "ddsource":
			d.Source, err = dec.Str()
		case "ddtags":
			val, err := dec.Str()
			if err != nil {
				return errors.NewUnmarshalError(err)
			}
			for _, match := range tagPattern.FindAllStringSubmatch(val, -1) {
				d.Tags = append(d.Tags, []string{match[1], match[2]})
			}
		case "hostname":
			d.Hostname, err = dec.Str()
		case "message":
			d.Message, err = dec.Str()
		case "service":
			d.Service, err = dec.Str()
		case "timestamp":
			d.TsMs, err = dec.Int64()
		case "source_type":
			d.SourceType, err = dec.Str()
		default:
			err = dec.Skip()
		}
		return err
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	for _, l := range [][]string{
		{"ddsource", d.Source},
		{"service", d.Service},
		{"hostname", d.Hostname},
		{"source_type", d.SourceType},
		{"type", "datadog"},
	} {
		if l[1] != "" {
			d.Tags = append(d.Tags, l)
		}
	}

	t := time.Now()
	if d.TsMs != 0 {
		t = time.Unix(d.TsMs/1000, d.TsMs%1000*1000000)
	}
	return d.onEntries(d.Tags, []int64{t.UnixNano()}, []string{d.Message}, []float64{0},
		[]uint8{model.SAMPLE_TYPE_LOG})
}

var UnmarshallDatadogV2JSONV2 = Build(
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &datadogRequestDec{ctx: ctx}
	}))
