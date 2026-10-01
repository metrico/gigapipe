package unmarshal

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/scanner"
	"time"
	"unsafe"

	"github.com/go-faster/city"
	"github.com/go-faster/jx"
	clcwriter "github.com/metrico/cloki-config/config/writer"
	"github.com/metrico/qryn/v5/writer/config"
	"github.com/metrico/qryn/v5/writer/metric"
	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils/errors"
	"github.com/metrico/qryn/v5/writer/utils/helputils"
	"github.com/metrico/qryn/v5/writer/utils/helputils/cityhash102"
	"github.com/metrico/qryn/v5/writer/utils/logger"
)

// RejectLokiPushValue is the counted reason for a Loki JSON push rejected
// because an entry carries a value.
const RejectLokiPushValue = "loki_push_value"

type lokiStream struct {
	labels [][]string
	tsNs   []int64
	lines  []string
}

// pushRequestDec decodes a Loki JSON push. The whole request is decoded before
// any entry is emitted, and an entry carrying a value fails it with a 400.
type pushRequestDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler

	streams []lokiStream
	valueAt int
}

func (p *pushRequestDec) Decode() error {
	d := jx.Decode(p.ctx.bodyReader, 64*1024)
	err := jsonParseError(d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "streams":
			return d.Arr(func(d *jx.Decoder) error {
				p.streams = append(p.streams, lokiStream{})
				p.valueAt = -1
				s := &p.streams[len(p.streams)-1]
				if err := p.decodeStream(d, s); err != nil {
					return err
				}
				if p.valueAt >= 0 {
					metric.IngestRejected.WithLabelValues(RejectLokiPushValue).Inc()
					return errors.New400Error(fmt.Sprintf(
						"stream %s: entry %d carries a value; the Loki push API accepts log lines only",
						formatLokiLabels(s.labels), p.valueAt))
				}
				return nil
			})
		default:
			d.Skip()
		}
		return nil
	}))
	if err != nil {
		return err
	}
	for _, s := range p.streams {
		err := p.onEntries(s.labels, s.tsNs, s.lines, make([]float64, len(s.tsNs)),
			slices.Repeat([]uint8{model.SAMPLE_TYPE_LOG}, len(s.tsNs)))
		if err != nil {
			return err
		}
	}
	return nil
}

func formatLokiLabels(lbls [][]string) string {
	parts := make([]string, len(lbls))
	for i, l := range lbls {
		parts[i] = l[0] + "=" + strconv.Quote(l[1])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (p *pushRequestDec) SetOnEntries(h onEntriesHandler) {
	p.onEntries = h
}

func (p *pushRequestDec) decodeStream(d *jx.Decoder, s *lokiStream) error {
	err := d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "stream":
			return p.decodeStreamStream(d, s)
		case "labels":
			return p.decodeStreamLabels(d, s)
		case "values":
			return d.Arr(func(d *jx.Decoder) error {
				return p.decodeStreamValue(d, s)
			})
		case "entries":
			return d.Arr(func(d *jx.Decoder) error {
				return p.decodeStreamEntry(d, s)
			})
		default:
			d.Skip()
		}
		return nil
	})
	return err
}

func (p *pushRequestDec) decodeStreamStream(d *jx.Decoder, s *lokiStream) error {
	err := d.Obj(func(d *jx.Decoder, key string) error {
		val, err := d.Str()
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		s.labels = append(s.labels, []string{key, val})
		return nil
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	s.labels = sanitizeLabels(s.labels)

	return nil
}

func (p *pushRequestDec) decodeStreamLabels(d *jx.Decoder, s *lokiStream) error {
	labelsBytes, err := d.StrBytes()
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	s.labels, err = parseLabelsLokiFormat(labelsBytes, s.labels)
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	s.labels = sanitizeLabels(s.labels)
	return err
}

// markValue records the stream's first entry that carries a value.
func (p *pushRequestDec) markValue(s *lokiStream) {
	if p.valueAt < 0 {
		p.valueAt = len(s.tsNs)
	}
}

func (p *pushRequestDec) decodeStreamValue(d *jx.Decoder, s *lokiStream) error {
	j := -1
	var (
		tsNs int64
		str  string
		err  error
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
			return err
		case 2:
			if d.Next() == jx.Number {
				p.markValue(s)
			}
			return d.Skip()
		default:
			d.Skip()
		}
		return nil
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	s.tsNs = append(s.tsNs, tsNs)
	s.lines = append(s.lines, str)
	return nil
}

func (p *pushRequestDec) decodeStreamEntry(d *jx.Decoder, s *lokiStream) error {
	var (
		tsNs int64
		str  string
	)
	err := d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "ts", "timestamp":
			bTs, err := d.StrBytes()
			if err != nil {
				return err
			}
			tsNs, err = parseTime(bTs)
			return err
		case "line":
			var err error
			str, err = d.Str()
			return err
		case "value":
			p.markValue(s)
			return d.Skip()
		default:
			return d.Skip()
		}
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}

	s.tsNs = append(s.tsNs, tsNs)
	s.lines = append(s.lines, str)
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
	//2021-12-26T16:00:06.944Z
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
