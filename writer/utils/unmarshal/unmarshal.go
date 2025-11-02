package unmarshal

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/scanner"
	"time"
	"unsafe"

	"github.com/go-faster/city"
	"github.com/go-faster/jx"
	clcwriter "github.com/metrico/cloki-config/config/writer"
	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/writer/config"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils/errors"
	"github.com/metrico/qryn/v4/writer/utils/helputils"
	"github.com/metrico/qryn/v4/writer/utils/helputils/cityhash102"
)

type pushRequestDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler

	Labels [][]string

	TsNs   []int64
	String []string
	Value  []float64
	Types  []uint8
}

func (p *pushRequestDec) Decode() error {
	p.TsNs = make([]int64, 0, 1000)
	p.String = make([]string, 0, 1000)
	p.Value = make([]float64, 0, 1000)
	p.Labels = make([][]string, 0, 10)
	p.Types = make([]uint8, 0, 1000)

	d := jx.Decode(p.ctx.bodyReader, 64*1024)
	return jsonParseError(d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "streams":
			return d.Arr(func(d *jx.Decoder) error {
				p.TsNs = p.TsNs[:0]
				p.String = p.String[:0]
				p.Value = p.Value[:0]
				p.Labels = p.Labels[:0]
				p.Types = p.Types[:0]

				err := p.decodeStream(d)
				if err != nil {
					return err
				}
				return p.onEntries(p.Labels, p.TsNs, p.String, p.Value, p.Types)
			})
		default:
			d.Skip()
		}
		return nil
	}))
}

func (p *pushRequestDec) SetOnEntries(h onEntriesHandler) {
	p.onEntries = h
}

func (p *pushRequestDec) decodeStream(d *jx.Decoder) error {
	err := d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "stream":
			return p.decodeStreamStream(d)
		case "labels":
			return p.decodeStreamLabels(d)
		case "values":
			return p.decodeStreamValues(d)
		case "entries":
			return p.decodeStreamEntries(d)
		default:
			d.Skip()
		}
		return nil
	})
	return err
}

func (p *pushRequestDec) decodeStreamStream(d *jx.Decoder) error {
	err := d.Obj(func(d *jx.Decoder, key string) error {
		val, err := d.Str()
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		p.Labels = append(p.Labels, []string{key, val})
		return nil
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	p.Labels = sanitizeLabels(p.Labels)

	return nil
}

func (p *pushRequestDec) decodeStreamLabels(d *jx.Decoder) error {
	labelsBytes, err := d.StrBytes()
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	p.Labels, err = parseLabelsLokiFormat(labelsBytes, p.Labels)
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	p.Labels = sanitizeLabels(p.Labels)
	return err
}

func (p *pushRequestDec) decodeStreamValues(d *jx.Decoder) error {
	return d.Arr(func(d *jx.Decoder) error {
		return p.decodeStreamValue(d)
	})
}

func (p *pushRequestDec) decodeStreamValue(d *jx.Decoder) error {
	j := -1
	var (
		tsNs int64
		str  string
		val  float64
		err  error
		tp   uint8
	)
	err = d.Arr(func(d *jx.Decoder) error {
		j++
		switch j {
		case 0:
			strTsNs, err := d.Str()
			if err != nil {
				return errors.NewUnmarshalError(err)
			}
			tsNs, err = strconv.ParseInt(strTsNs, 10, 64)
			return err
		case 1:
			str, err = d.Str()
			tp |= model.SAMPLE_TYPE_LOG
			return err
		case 2:
			if d.Next() != jx.Number {
				return d.Skip()
			}
			val, err = d.Float64()
			tp |= model.SAMPLE_TYPE_METRIC
			return err
		default:
			d.Skip()
		}
		return nil
	})

	if tp == 3 {
		tp = 0
	}

	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	p.TsNs = append(p.TsNs, tsNs)
	p.String = append(p.String, str)
	p.Value = append(p.Value, val)
	p.Types = append(p.Types, tp)

	return nil
}

func (p *pushRequestDec) decodeStreamEntries(d *jx.Decoder) error {
	return d.Arr(func(d *jx.Decoder) error {
		return p.decodeStreamEntry(d)
	})
}

func (p *pushRequestDec) decodeStreamEntry(d *jx.Decoder) error {
	var (
		tsNs int64
		str  string
		val  float64
		err  error
		tp   uint8
	)
	err = d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "ts":
			bTs, err := d.StrBytes()
			if err != nil {
				return err
			}
			tsNs, err = parseTime(bTs)
			return err
		case "timestamp":
			bTs, err := d.StrBytes()
			if err != nil {
				return err
			}
			tsNs, err = parseTime(bTs)
			return err
		case "line":
			str, err = d.Str()
			tp |= model.SAMPLE_TYPE_LOG
			return err
		case "value":
			val, err = d.Float64()
			tp |= model.SAMPLE_TYPE_METRIC
			return err
		default:
			return d.Skip()
		}
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	if tp == 3 {
		tp = 0
	}

	p.TsNs = append(p.TsNs, tsNs)
	p.String = append(p.String, str)
	p.Value = append(p.Value, val)
	p.Types = append(p.Types, tp)
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	return nil
}

var DecodePushRequestStringV2 = Build(
	withLogsParser(func(ctx *ParserCtx) iLogsParser { return &pushRequestDec{ctx: ctx} }))

func encodeLabels(lbls [][]string) string {
	arrLbls := make([]string, len(lbls))
	for i, l := range lbls {
		arrLbls[i] = fmt.Sprintf("%s:%s", strconv.Quote(l[0]), strconv.Quote(l[1]))
	}
	return fmt.Sprintf("{%s}", strings.Join(arrLbls, ","))
}

func fingerprintLabels(lbls [][]string) uint64 {
	determs := []uint64{0, 0, 1}
	for _, lbl := range lbls {
		hash := cityhash102.Hash128to64(cityhash102.Uint128{
			city.CH64([]byte(lbl[0])),
			city.CH64([]byte(lbl[1])),
		})
		determs[0] = determs[0] + hash
		determs[1] = determs[1] ^ hash
		determs[2] = determs[2] * (1779033703 + 2*hash)
	}
	fingerByte := unsafe.Slice((*byte)(unsafe.Pointer(&determs[0])), 24)
	var fingerPrint uint64
	switch config.Cloki.Setting.FingerPrintType {
	case clcwriter.FINGERPRINT_CityHash:
		fingerPrint = city.CH64(fingerByte)
	case clcwriter.FINGERPRINT_Bernstein:
		fingerPrint = uint64(helputils.FingerprintLabelsDJBHashPrometheus(fingerByte))
	}
	return fingerPrint
}

var sanitizeRe = regexp.MustCompile("(^[^a-zA-Z_]|[^a-zA-Z0-9_])")

func sanitizeLabels(lbls [][]string) [][]string {
	for i := range lbls {
		lbls[i][0] = sanitizeRe.ReplaceAllString(lbls[i][0], "_")
		if len(lbls[i][1]) > 100 {
			lbls[i][1] = lbls[i][1][:100] + "..."
		}
	}
	return lbls
}

func parseTime(b []byte) (int64, error) {
	// 2021-12-26T16:00:06.944Z
	var err error
	if b != nil {
		var timestamp int64
		val := string(b)
		if strings.ContainsAny(val, ":-TZ") {
			t, e := time.Parse(time.RFC3339, val)
			if e != nil {

				logger.Debug("ERROR unmarshaling this string: ", e.Error())
				return 0, errors.NewUnmarshalError(e)
			}
			return t.UTC().UnixNano(), nil
		} else {
			timestamp, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				logger.Debug("ERROR unmarshaling this NS: ", val, err)
				return 0, errors.NewUnmarshalError(err)
			}
		}
		return timestamp, nil
	} else {
		err = fmt.Errorf("bad byte array for Unmarshaling")
		logger.Debug("bad data: ", err)
		return 0, errors.NewUnmarshalError(err)
	}
}

func parseLabelsLokiFormat(labels []byte, buf [][]string) ([][]string, error) {
	s := scanner.Scanner{}
	s.Init(bytes.NewReader(labels))
	errorF := func() ([][]string, error) {
		return nil, fmt.Errorf("unknown input: %s", labels[s.Offset:])
	}
	tok := s.Scan()
	checkRune := func(expect rune, strExpect string) bool {
		return tok == expect && (strExpect == "" || s.TokenText() == strExpect)
	}
	if !checkRune(123, "{") {
		return errorF()
	}
	for tok != scanner.EOF {
		tok = s.Scan()
		if !checkRune(scanner.Ident, "") {
			return errorF()
		}
		name := s.TokenText()
		tok = s.Scan()
		if !checkRune(61, "=") {
			return errorF()
		}
		tok = s.Scan()
		if !checkRune(scanner.String, "") {
			return errorF()
		}
		val, err := strconv.Unquote(s.TokenText())
		if err != nil {
			return nil, errors.NewUnmarshalError(err)
		}
		tok = s.Scan()
		buf = append(buf, []string{name, val})
		if checkRune(125, "}") {
			return buf, nil
		}
		if !checkRune(44, ",") {
			return errorF()
		}
	}
	return buf, nil
}
