package unmarshal

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"time"
	"unsafe"

	"github.com/go-faster/city"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils"
	"github.com/metrico/qryn/v4/writer/utils/logger"
	"github.com/metrico/qryn/v4/writer/utils/metadata"
	"github.com/metrico/qryn/v4/writer/utils/numbercache"
	"google.golang.org/protobuf/proto"
)

// OrgChecker defines the interface for checking fingerprints.
type OrgChecker interface {
	IsFPAllowed(fp uint64) bool
	IsSus() bool
}

type OrgCheckerFactory interface {
	CreateOrgChecker() OrgChecker
}

type onEntriesHandler func(labels [][]string, timestampsNS []int64,
	message []string, value []float64, types []uint8) error

type onProfileHandler func(timestampNs uint64,
	Type string,
	serviceName string,
	samplesTypesUnits []model.StrStr, periodType string,
	periodUnit string, tags []model.StrStr,
	durationNs uint64, payloadType string, payload []byte,
	valuersAgg []model.ValuesAgg,
	tree []model.TreeRootStructure, functions []model.Function) error

type onSpanHandler func(traceId []byte, spanId []byte, timestampNs int64, durationNs int64,
	parentId string, name string, serviceName string, payload []byte, key []string, val []string) error

type ParsingFunction func(ctx context.Context, body io.Reader,
	fpCache numbercache.ICache[uint64]) chan *model.ParserResponse

type ParserCtx struct {
	bodyReader io.Reader
	bodyBuffer []byte
	bodyObject interface{}
	fpCache    numbercache.ICache[uint64]
	ctx        context.Context
	ctxMap     map[utils.ContextKey]string
}

type parserFn func(ctx *ParserCtx) error

type iLogsParser interface {
	Decode() error
	SetOnEntries(h onEntriesHandler)
}

type iProfilesParser interface {
	Decode() error
	SetOnProfile(h onProfileHandler)
}
type iSpansParser interface {
	Decode() error
	SetOnEntry(h onSpanHandler)
}

type parserBuilder struct {
	PreParse      []parserFn
	LogsParser    func(ctx *ParserCtx) iLogsParser
	ProfileParser func(ctx *ParserCtx) iProfilesParser
	SpansParser   func(ctx *ParserCtx) iSpansParser
	payloadType   int8
}

type parserDoer struct {
	PreParse      []parserFn
	LogsParser    iLogsParser
	SpansParser   iSpansParser
	ProfileParser iProfilesParser
	ctx           *ParserCtx
	ttlDays       uint16

	res         chan *model.ParserResponse
	tsSpl       *timeSeriesAndSamples
	size        int
	payloadType int8
	profile     *model.ProfileData
	spans       *model.TempoSamples
	attrs       *model.TempoTag
}

func (p *parserDoer) Do() chan *model.ParserResponse {
	p.res = make(chan *model.ParserResponse)
	for _, fn := range p.PreParse {
		err := fn(p.ctx)
		if err != nil {
			go func() { p.res <- &model.ParserResponse{Error: err}; close(p.res) }()
			return p.res
		}
	}

	if p.LogsParser != nil {
		p.doParseLogs()
	} else if p.SpansParser != nil {
		p.doParseSpans()
	} else if p.ProfileParser != nil {
		p.doParseProfile()
	}

	return p.res
}

func (p *parserDoer) doParseProfile() {
	parser := p.ProfileParser

	parser.SetOnProfile(p.onProfile)
	p.size = 0
	p.resetProfile()

	go func() {
		defer p.tamePanic()
		err := parser.Decode()
		if err != nil {
			p.res <- &model.ParserResponse{Error: err}
			close(p.res)
			return
		}
		p.res <- &model.ParserResponse{
			ProfileRequest: p.profile,
		}

		close(p.res)
	}()
}

func (p *parserDoer) resetProfile() {
	p.profile = &model.ProfileData{}
}
func (p *parserDoer) doParseLogs() {
	parser := p.LogsParser
	meta := ""
	_meta := p.ctx.ctx.Value(utils.ContextKeyMeta)
	if _meta != nil {
		meta = _meta.(string)
	}

	p.ttlDays = 0
	ttlDays := p.ctx.ctx.Value(utils.ContextKeyTTLDays)
	if ttlDays != nil {
		p.ttlDays = ttlDays.(uint16)
	}

	p.tsSpl = newTimeSeriesAndSamples(p.res, meta)

	parser.SetOnEntries(p.onEntries)
	p.tsSpl.reset()

	go func() {
		defer p.tamePanic()
		err := parser.Decode()
		if err != nil {
			p.res <- &model.ParserResponse{Error: err}
			close(p.res)
			return
		}
		p.tsSpl.flush()
		p.tsSpl.reset()
		close(p.res)
	}()
}

func (p *parserDoer) doParseSpans() {
	parser := p.SpansParser
	parser.SetOnEntry(p.onSpan)

	p.size = 0
	p.resetSpans()

	go func() {
		defer p.tamePanic()
		err := parser.Decode()
		if err != nil {
			p.res <- &model.ParserResponse{Error: err}
			close(p.res)
			return
		}
		p.res <- &model.ParserResponse{
			SpansRequest:      p.spans,
			SpansAttrsRequest: p.attrs,
		}
		close(p.res)
	}()
}

func (p *parserDoer) tamePanic() {
	if err := recover(); err != nil {
		logger.Error(err, " stack:", string(debug.Stack()))
		p.res <- &model.ParserResponse{Error: fmt.Errorf("panic: %v", err)}
		close(p.res)
		recover()
	}
}
func (p *parserDoer) resetSpans() {
	p.spans = &model.TempoSamples{}
	p.attrs = &model.TempoTag{}
}

func (p *parserDoer) onProfile(timestampNs uint64,
	Type string,
	serviceName string,
	samplesTypesUnits []model.StrStr, periodType string,
	periodUnit string, tags []model.StrStr,
	durationNs uint64, payloadType string, payload []byte,
	valuersAgg []model.ValuesAgg, tree []model.TreeRootStructure, functions []model.Function) error {
	p.profile.TimestampNs = append(p.profile.TimestampNs, timestampNs)
	p.profile.Ptype = append(p.profile.Ptype, Type)
	p.profile.ServiceName = append(p.profile.ServiceName, serviceName)
	p.profile.PeriodType = append(p.profile.PeriodType, periodType)
	p.profile.PeriodUnit = append(p.profile.PeriodUnit, periodUnit)
	p.profile.DurationNs = append(p.profile.DurationNs, durationNs)
	p.profile.PayloadType = append(p.profile.PayloadType, payloadType)
	p.profile.Payload = append(p.profile.Payload, payload)
	p.profile.SamplesTypesUnits = samplesTypesUnits
	p.profile.Tags = tags
	p.profile.ValuesAgg = valuersAgg
	p.profile.Function = functions
	p.profile.Tree = tree

	p.profile.Size = p.calculateProfileSize()

	if p.profile.Size > 1*1024*1024 {
		p.res <- &model.ParserResponse{
			SpansRequest:      p.spans,
			SpansAttrsRequest: p.attrs,
		}
		p.resetProfile()
	}
	//p.res <- &model.ParserResponse{
	//	ProfileRequest: p.profile,
	//}

	return nil
}
func (p *parserDoer) calculateProfileSize() int {
	size := 0

	// Add sizes for all slices
	size += 8 // timestampNs (uint64)
	size += len(p.profile.Ptype)
	size += len(p.profile.ServiceName)
	size += len(p.profile.PeriodType)
	size += len(p.profile.PeriodUnit)
	size += 8 // durationNs (uint64)
	size += len(p.profile.PayloadType)
	size += len(p.profile.Payload)

	// Calculate size for slices of struct arrays
	for _, st := range p.profile.SamplesTypesUnits {
		size += len(st.Str1) + len(st.Str2)
	}
	for _, tag := range p.profile.Tags {
		size += len(tag.Str2) + len(tag.Str1)
	}

	// Accumulate the size
	return size

}

var serviceNameCandidates = map[string]bool{
	"service":                true,
	"app":                    true,
	"application":            true,
	"app_name":               true,
	"name":                   true,
	"app_kubernetes_io_name": true,
	"container":              true,
	"container_name":         true,
	"k8s_container_name":     true,
	"component":              true,
	"workload":               true,
	"job":                    true,
	"k8s_job_name":           true,
}

func (p *parserDoer) discoverServiceName(labels *[][]string) {
	serviceNameExists := false
	serviceName := "unknown"
	for _, l := range *labels {
		if l[0] == "service_name" {
			serviceNameExists = true
			serviceName = ""
			break
		}
		if serviceNameCandidates[l[0]] {
			serviceName = l[1]
		}
	}
	if !serviceNameExists && serviceName != "" {
		*labels = append(*labels, []string{"service_name", serviceName})
	}
}

func (p *parserDoer) onEntries(labels [][]string, timestampsNS []int64,
	message []string, value []float64, types []uint8) error {

	ttlDays := p.ttlDays
	var metricName string
	var metadataFromLabels map[string]*metadata.MetricMetadata
	var labelsToRemove map[string]bool

	// Extract metadata from labels and find metric name
	if ttlDays == 0 {
		var _labels [][]string
		for _, lbl := range labels {
			if lbl[0] == "__ttl_days__" {
				_ttlDays, err := strconv.ParseInt(lbl[1], 10, 16)
				if err == nil {
					ttlDays = uint16(_ttlDays)
				}
				continue
			}
			if lbl[0] == "__name__" {
				metricName = lbl[1]
			}
			_labels = append(_labels, lbl)
		}
		labels = _labels
	} else {
		// Find metric name even if ttlDays is set
		for _, lbl := range labels {
			if lbl[0] == "__name__" {
				metricName = lbl[1]
				break
			}
		}
	}

	// Extract metadata from special labels
	metadataFromLabels, labelsToRemove = metadata.ExtractMetadataFromLabels(labels)

	// Remove metadata labels before processing
	if len(labelsToRemove) > 0 {
		var cleanedLabels [][]string
		for _, lbl := range labels {
			if !labelsToRemove[lbl[0]] {
				cleanedLabels = append(cleanedLabels, lbl)
			}
		}
		labels = cleanedLabels
	}

	p.discoverServiceName(&labels)

	dates := map[time.Time]bool{}
	fp := fingerprintLabels(labels)

	p.tsSpl.spl.MMessage = append(p.tsSpl.spl.MMessage, message...)
	p.tsSpl.spl.MValue = append(p.tsSpl.spl.MValue, value...)
	p.tsSpl.spl.MTimestampNS = append(p.tsSpl.spl.MTimestampNS, timestampsNS...)
	p.tsSpl.spl.MFingerprint = append(p.tsSpl.spl.MFingerprint, fastFillArray(len(timestampsNS), fp)...)
	p.tsSpl.spl.MTTLDays = append(p.tsSpl.spl.MTTLDays, fastFillArray(len(timestampsNS), ttlDays)...)
	p.tsSpl.spl.MType = append(p.tsSpl.spl.MType, types...)

	var tps [3]bool
	for _, t := range types {
		tps[t] = true
	}

	for i, tsns := range timestampsNS {
		dates[time.Unix(tsns/1000000000, 0).Truncate(time.Hour*24)] = true
		p.tsSpl.spl.Size += len(message[i]) + 26
	}

	// Process metadata: extract from labels
	var finalMetadata *metadata.MetricMetadata
	if metricName != "" && len(metadataFromLabels) > 0 {
		// Use metadata from labels
		if md, ok := metadataFromLabels[metricName]; ok {
			finalMetadata = md
		}
	}

	// Store metadata if found and not already stored for this metric name
	if finalMetadata != nil {
		// Check if we already have metadata for this metric name in current batch
		alreadyStored := false
		for _, storedName := range p.tsSpl.metadata.MetricNames {
			if storedName == metricName {
				alreadyStored = true
				break
			}
		}

		if !alreadyStored {
			metadataJSON, err := finalMetadata.ToJSON()
			if err == nil {
				p.tsSpl.metadata.MetricNames = append(p.tsSpl.metadata.MetricNames, metricName)
				p.tsSpl.metadata.MetadataJSON = append(p.tsSpl.metadata.MetadataJSON, metadataJSON)
				p.tsSpl.metadata.Size += len(metadataJSON) + len(metricName) // metric_name + json size
			}
		}
	}

	for d := range dates {
		if maybeAddFp(d, fp, p.ctx.fpCache) {
			_labels := encodeLabels(labels)
			for t := range tps {
				if !tps[t] {
					continue
				}

				p.tsSpl.ts.MDate = append(p.tsSpl.ts.MDate, d)
				p.tsSpl.ts.MLabels = append(p.tsSpl.ts.MLabels, _labels)
				p.tsSpl.ts.MFingerprint = append(p.tsSpl.ts.MFingerprint, fp)
				p.tsSpl.ts.MType = append(p.tsSpl.ts.MType, uint8(t))
				p.tsSpl.ts.MTTLDays = append(p.tsSpl.ts.MTTLDays, ttlDays)
				p.tsSpl.ts.Size += 14 + len(_labels)
			}
		}
	}

	if p.tsSpl.spl.Size+p.tsSpl.ts.Size > 1*1024*1024 {
		p.tsSpl.flush()
		p.tsSpl.reset()
	}

	return nil
}

func (p *parserDoer) onSpan(traceId []byte, spanId []byte, timestampNs int64, durationNs int64,
	parentId string, name string, serviceName string, payload []byte, key []string, val []string) error {
	p.spans.MTraceId = append(p.spans.MTraceId, traceId)
	p.spans.MSpanId = append(p.spans.MSpanId, spanId)
	p.spans.MTimestampNs = append(p.spans.MTimestampNs, timestampNs)
	p.spans.MDurationNs = append(p.spans.MDurationNs, durationNs)
	p.spans.MParentId = append(p.spans.MParentId, parentId)
	p.spans.MName = append(p.spans.MName, name)
	p.spans.MServiceName = append(p.spans.MServiceName, serviceName)
	p.spans.MPayloadType = append(p.spans.MPayloadType, p.payloadType)
	p.spans.MPayload = append(p.spans.MPayload, payload)

	p.spans.Size += 49 + len(parentId) + len(name) + len(serviceName) + len(payload)

	for i, k := range key {
		p.attrs.MTraceId = append(p.attrs.MTraceId, traceId)
		p.attrs.MSpanId = append(p.attrs.MSpanId, spanId)
		p.attrs.MTimestampNs = append(p.attrs.MTimestampNs, timestampNs)
		p.attrs.MDurationNs = append(p.attrs.MDurationNs, durationNs)
		p.attrs.MKey = append(p.attrs.MKey, k)
		p.attrs.MVal = append(p.attrs.MVal, val[i])
		p.attrs.MDate = append(p.attrs.MDate, time.Unix(timestampNs/1000000000, 0))
		p.attrs.Size += 40 + len(k) + len(val[i])
	}

	if p.attrs.Size+p.spans.Size > 1*1024*1024 {
		p.res <- &model.ParserResponse{
			SpansRequest:      p.spans,
			SpansAttrsRequest: p.attrs,
		}
		p.resetSpans()
	}

	return nil
}

type buildOption func(builder *parserBuilder) *parserBuilder

func Build(options ...buildOption) ParsingFunction {
	builder := &parserBuilder{}
	for _, o := range options {
		builder = o(builder)
	}
	return func(ctx context.Context, body io.Reader, fpCache numbercache.ICache[uint64]) chan *model.ParserResponse {
		doer := &parserDoer{
			ctx: &ParserCtx{
				bodyReader: body,
				fpCache:    fpCache,
				ctx:        ctx,
				ctxMap:     map[utils.ContextKey]string{},
			},
			PreParse:    builder.PreParse,
			payloadType: builder.payloadType,
		}
		if builder.LogsParser != nil {
			doer.LogsParser = builder.LogsParser(doer.ctx)
		} else if builder.SpansParser != nil {
			doer.SpansParser = builder.SpansParser(doer.ctx)
		} else {
			doer.ProfileParser = builder.ProfileParser(doer.ctx)
		}
		return doer.Do()
	}
}
func withProfileParser(fn func(ctx *ParserCtx) iProfilesParser) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.ProfileParser = fn
		return builder
	}
}
func withLogsParser(fn func(ctx *ParserCtx) iLogsParser) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.LogsParser = fn
		return builder
	}
}

func withSpansParser(fn func(ctx *ParserCtx) iSpansParser) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.SpansParser = fn
		return builder
	}
}

func withStringValueFromCtx(key utils.ContextKey) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.PreParse = append(builder.PreParse, func(ctx *ParserCtx) error {
			res := ctx.ctx.Value(key)
			if res != nil {
				ctx.ctxMap[key] = res.(string)
			}
			return nil
		})
		return builder
	}
}

var withBufferedBody buildOption = func(builder *parserBuilder) *parserBuilder {
	builder.PreParse = append(builder.PreParse, func(ctx *ParserCtx) error {
		var err error
		ctx.bodyBuffer, err = io.ReadAll(ctx.bodyReader)
		if err != nil {
			return err
		}
		ctx.bodyReader = nil
		return nil
	})
	return builder
}

func withParsedBody(fn func() proto.Message) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.PreParse = append(builder.PreParse, func(ctx *ParserCtx) error {
			obj := fn()
			err := proto.Unmarshal(ctx.bodyBuffer, obj)
			if err != nil {
				return err
			}
			ctx.bodyObject = obj
			return nil
		})
		return builder
	}
}

func withPayloadType(tp int8) buildOption {
	return func(builder *parserBuilder) *parserBuilder {
		builder.payloadType = tp
		return builder
	}
}

func maybeAddFp(date time.Time, fp uint64, fpCache numbercache.ICache[uint64]) bool {
	dateTS := date.Unix()
	var bs [16]byte
	copy(bs[0:8], unsafe.Slice((*byte)(unsafe.Pointer(&dateTS)), 16))
	copy(bs[8:16], unsafe.Slice((*byte)(unsafe.Pointer(&fp)), 16))
	_fp := city.CH64(bs[:])
	return !fpCache.CheckAndSet(_fp)
}
