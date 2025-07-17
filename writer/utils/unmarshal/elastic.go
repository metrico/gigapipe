package unmarshal

import (
	"bufio"
	"github.com/go-faster/jx"
	"github.com/metrico/qryn/writer/model"
	heputils "github.com/metrico/qryn/writer/utils"
	customErrors "github.com/metrico/qryn/writer/utils/errors"
	"github.com/metrico/qryn/writer/utils/numbercache"
	"io"
	"time"
)

type ElasticUnmarshalOpts struct {
	DB         string
	Body       []byte
	BodyStream io.Reader
	Target     string
	ID         string
	FPCache    numbercache.ICache[uint64]
}

type ElasticUnmarshal struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler
}

func (e *ElasticUnmarshal) Decode() error {
	labels := [][]string{{"type", "elastic"}, {"_index", e.ctx.ctxMap[heputils.ContextKeyTarget]}}
	if id, ok := e.ctx.ctxMap[heputils.ContextKeyID]; ok {
		labels = append(labels, []string{"_id", id})
	}
	return e.onEntries(labels, []int64{time.Now().UnixNano()}, []string{string(e.ctx.bodyBuffer)}, []float64{0},
		[]uint8{model.SAMPLE_TYPE_LOG})
}
func (e *ElasticUnmarshal) SetOnEntries(h onEntriesHandler) {
	e.onEntries = h
}

var ElasticDocUnmarshalV2 = Build(
	withStringValueFromCtx(heputils.ContextKeyTarget),
	withStringValueFromCtx(heputils.ContextKeyID),
	withBufferedBody,
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &ElasticUnmarshal{ctx: ctx}
	}))

type elasticBulkDec struct {
	ctx       *ParserCtx
	onEntries onEntriesHandler

	labels [][]string
}

func (e *elasticBulkDec) Decode() error {
	scanner := bufio.NewScanner(e.ctx.bodyReader)
	for scanner.Scan() {
		err := e.decodeLine(scanner.Bytes())
		if err != nil {
			return customErrors.NewUnmarshalError(err)
		}
	}
	return nil
}

func (e *elasticBulkDec) SetOnEntries(h onEntriesHandler) {
	e.onEntries = h
}

func (e *elasticBulkDec) decodeLine(line []byte) error {
	dec := jx.DecodeBytes(line)
	noContent := false
	// Check if the line is empty or not a valid JSON
	if len(line) == 0 {
		return nil
	}
	err := dec.Obj(func(d *jx.Decoder, key string) error {
		if noContent {
			return dec.Skip()
		}
		switch key {
		case "delete":
			noContent = true
			e.labels = e.labels[:0]
			// Skip remaining content for delete operation
			return d.Skip()
		case "update":
			noContent = true
			e.labels = e.labels[:0]
			// Skip remaining content for update operation
			return d.Skip()
		case "index", "create":
			noContent = true
			return e.decodeCreateObj(d)
		default:
			// Handle unexpected keys
			return d.Skip()
		}
	})

	if err != nil {
		//	return fmt.Errorf("error decoding line: %w", err)
		return customErrors.NewUnmarshalError(err)
	}

	// Ensure `e.labels` is processed correctly
	if noContent || len(e.labels) == 0 {
		return nil
	}

	// Invoke onEntries with the processed data
	return e.onEntries(e.labels, []int64{time.Now().UnixNano()}, []string{string(line)}, []float64{0},
		[]uint8{model.SAMPLE_TYPE_LOG})
}

func (e *elasticBulkDec) decodeCreateObj(dec *jx.Decoder) error {
	target := e.ctx.ctxMap[heputils.ContextKeyTarget]
	e.labels = [][]string{{"type", "elastic"}}
	if target != "" {
		e.labels = append(e.labels, []string{"_index", target})
	}
	return dec.Obj(func(d *jx.Decoder, key string) error {
		tp := d.Next()
		if tp != jx.String {
			return d.Skip()
		}
		if (target != "" && key == "_index") || key == "type" {
			return d.Skip()
		}
		val, err := dec.Str()
		if err != nil {
			return customErrors.NewUnmarshalError(err)
		}
		e.labels = append(e.labels, []string{key, val})
		return nil
	})
}

var ElasticBulkUnmarshalV2 = Build(
	withStringValueFromCtx(heputils.ContextKeyTarget),
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &elasticBulkDec{ctx: ctx}
	}))
