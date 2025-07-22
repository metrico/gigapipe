package unmarshal

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"mime/multipart"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-faster/city"
	pprof_proto "github.com/google/pprof/profile"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/utils"
)

type SampleType struct {
	Key   string
	Sum   int64
	Count int32
}
type Label struct {
	Name, Value string
}

type PayloadType uint8

// Represents the high-level type of a profile
type ProfileType struct {
	Type       string
	PeriodType string
	PeriodUnit string
	SampleType []string
	SampleUnit []string
}
type ProfileIR struct {
	Type             ProfileType
	DurationNano     int64
	TimeStampNao     int64
	Payload          *bytes.Buffer
	PayloadType      PayloadType
	ValueAggregation interface{}
	Profile          *pprof_proto.Profile
}

type profTrieNode struct {
	parentId uint64
	funcId   uint64
	nodeId   uint64
	values   []profTrieValue
}

type profTrieValue struct {
	name  string
	self  int64
	total int64
}
type codec uint8

const (
	Gzip codec = iota
)

// Decodes compressed streams
type Decompressor struct {
	maxUncompressedSizeBytes int64
	decoders                 map[codec]func(body io.Reader) (io.Reader, error)
}

type pProfProtoDec struct {
	ctx                 *ParserCtx
	onProfiles          onProfileHandler
	uncompressedBufPool *sync.Pool
	decompressor        *Decompressor
}

func (p *pProfProtoDec) Decode() error {
	var timestampNs uint64
	var durationNs uint64
	var tags []model.StrStr
	fromValue := p.ctx.ctxMap[utils.ContextKeyFrom]
	start, err := strconv.ParseUint(fromValue, 10, 64)
	if err != nil {
		fmt.Println("st error", err.Error())
		return fmt.Errorf("failed to parse start time: %w", err)
	}

	endValue := p.ctx.ctxMap[utils.ContextKeyUntil]
	end, err := strconv.ParseUint(endValue, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse end time: %w", err)
	}
	name := p.ctx.ctxMap[utils.ContextKeyName]
	i := strings.Index(name, "{")
	length := len(name)
	if i < 0 {
		i = length
	} else {

		promqllike := name[i+1 : length-1] // stripe {}
		if len(promqllike) > 0 {
			words := strings.FieldsFunc(promqllike, func(r rune) bool { return r == '=' || r == ',' })
			sz := len(words)
			if sz == 0 || sz%2 != 0 {
				return fmt.Errorf("failed to compile labels")
			}

			for j := 0; j < len(words); j += 2 {
				tags = append(tags, model.StrStr{
					Str1: words[j],
					Str2: words[j+1],
				})
			}
		}
	}
	start = ns(start)
	end = ns(end)
	name = name[:i]
	durationNs = end - start
	timestampNs = start

	buf := acquireBuf(p.uncompressedBufPool)
	defer func() {
		releaseBuf(p.uncompressedBufPool, buf)
	}()

	data, err := io.ReadAll(p.ctx.bodyReader)
	if err != nil {
		fmt.Println("Error reading from reader:", err)
		return err
	}
	f, err := processMIMEData(string(data))
	if err != nil {
		fmt.Println("Error reading from reader:", err)
		return err
	}
	// Convert bytes to string
	err = p.decompressor.Decompress(f, Gzip, buf)
	if err != nil {
		return fmt.Errorf("failed to decompress body: %w", err)
	}

	ps, err := Parse(buf)
	if err != nil {
		return fmt.Errorf("failed to parse pprof: %w", err)
	}

	for _, profile := range ps {

		var sampleUnitArray []model.StrStr
		var functionArray []model.Function
		var treeArray []model.TreeRootStructure
		var ValuesAgg []model.ValuesAgg
		functions, tree := postProcessProf(profile.Profile)
		valueAgg := profile.ValueAggregation.([]SampleType)

		for i, sType := range profile.Type.SampleType {
			sampleUnitArray = append(sampleUnitArray, model.StrStr{
				Str1: sType,
				Str2: profile.Type.SampleUnit[i],
			})
		}

		for _, v := range valueAgg {
			ValuesAgg = append(ValuesAgg, model.ValuesAgg{
				ValueStr:   v.Key,
				ValueInt64: v.Sum,
				ValueInt32: v.Count,
			})
		}

		for _, f := range functions {
			function := model.Function{
				ValueInt64: f.ValueInt64,
				ValueStr:   f.ValueStr,
			}
			functionArray = append(functionArray, function)
		}

		for _, t := range tree {
			var valuesArr []model.ValuesArrTuple
			for _, v := range t.values {
				valuesArr = append(valuesArr, model.ValuesArrTuple{
					ValueStr:         v.name,
					FirstValueInt64:  v.self,
					SecondValueInt64: v.total,
				})
			}
			treeStruct := model.TreeRootStructure{
				Field1:        t.parentId,
				Field2:        t.funcId,
				Field3:        t.nodeId,
				ValueArrTuple: valuesArr,
			}

			treeArray = append(treeArray, treeStruct)

		}
		payload := profile.Payload.Bytes()
		payloadType := fmt.Sprint(profile.PayloadType)
		err = p.onProfiles(timestampNs, profile.Type.Type,
			name,
			sampleUnitArray,
			profile.Type.PeriodType,
			profile.Type.PeriodUnit,
			tags, durationNs, payloadType, payload, ValuesAgg, treeArray, functionArray)
		if err != nil {

			fmt.Println("Error at onProfiles")
			return err
		}
	}

	return nil
}

func (p *pProfProtoDec) SetOnProfile(h onProfileHandler) {
	p.onProfiles = h
	p.uncompressedBufPool = &sync.Pool{}
	p.decompressor = NewDecompressor(100000)
}

var UnmarshalProfileProtoV2 = Build(
	withStringValueFromCtx(utils.ContextKeyFrom),
	withStringValueFromCtx(utils.ContextKeyName),
	withStringValueFromCtx(utils.ContextKeyUntil),
	withProfileParser(func(ctx *ParserCtx) iProfilesParser {
		return &pProfProtoDec{ctx: ctx}
	}))

func acquireBuf(p *sync.Pool) *bytes.Buffer {
	v := p.Get()
	if v == nil {
		return new(bytes.Buffer)
	}
	return v.(*bytes.Buffer)
}
func releaseBuf(p *sync.Pool, buf *bytes.Buffer) {
	buf.Reset()
	p.Put(buf)
}

// Decodes the accepted reader, applying the configured size limit to avoid oom by compression bomb
func (d *Decompressor) Decompress(r io.Reader, c codec, out *bytes.Buffer) error {
	decoder, ok := d.decoders[c]
	if !ok {
		return fmt.Errorf("unsupported encoding")
	}

	dr, err := decoder(r)
	if err != nil {
		fmt.Println("error during decode........")
		return err
	}

	return d.readBytes(dr, out)
}

func (d *Decompressor) readBytes(r io.Reader, out *bytes.Buffer) error {
	// read max+1 to validate size via a single Read()
	lr := io.LimitReader(r, d.maxUncompressedSizeBytes+1)

	n, err := out.ReadFrom(lr)
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("empty profile")
	}
	if n > d.maxUncompressedSizeBytes {
		return fmt.Errorf("body size exceeds the limit %d bytes", d.maxUncompressedSizeBytes)
	}
	return nil
}

func Parse(data *bytes.Buffer) ([]ProfileIR, error) {
	// Parse pprof data
	pProfData, err := pprof_proto.Parse(data)
	if err != nil {
		return nil, err
	}

	// Process pprof data and create SampleType slice
	var sampleTypes []string
	var sampleUnits []string
	var valueAggregates []SampleType

	for i, st := range pProfData.SampleType {
		sampleTypes = append(sampleTypes, pProfData.SampleType[i].Type)
		sampleUnits = append(sampleUnits, pProfData.SampleType[i].Unit)
		sum, count := calculateSumAndCount(pProfData, i)
		valueAggregates = append(valueAggregates, SampleType{fmt.Sprintf("%s:%s", st.Type, st.Unit), sum, count})
	}

	var profiles []ProfileIR
	var profileType string

	switch pProfData.PeriodType.Type {
	case "cpu":
		profileType = "process_cpu"
	case "wall":
		profileType = "wall"
	case "mutex", "contentions":
		profileType = "mutex"
	case "goroutine":
		profileType = "goroutines"
	case "objects", "space", "alloc", "inuse":
		profileType = "memory"
	case "block":
		profileType = "block"
	}

	profileTypeInfo := ProfileType{
		PeriodType: pProfData.PeriodType.Type,
		PeriodUnit: pProfData.PeriodType.Unit,
		SampleType: sampleTypes,
		SampleUnit: sampleUnits,
		Type:       profileType,
	}

	// Create a new ProfileIR instance
	profile := ProfileIR{
		ValueAggregation: valueAggregates,
		Type:             profileTypeInfo,
		Profile:          pProfData,
	}
	profile.Payload = new(bytes.Buffer)
	pProfData.WriteUncompressed(profile.Payload)
	// Append the profile to the result
	profiles = append(profiles, profile)
	return profiles, nil
}

func calculateSumAndCount(samples *pprof_proto.Profile, sampleTypeIndex int) (int64, int32) {
	var sum int64
	count := int32(len(samples.Sample))
	for _, sample := range samples.Sample {
		// Check if the sample has a value for the specified sample type
		if sampleTypeIndex < len(sample.Value) {
			// Accumulate the value for the specified sample type
			sum += sample.Value[sampleTypeIndex]
		}
	}

	return sum, count
}

func postProcessProf(profile *pprof_proto.Profile) ([]*model.Function, []*profTrieNode) {
	funcs := map[uint64]string{}
	tree := map[uint64]*profTrieNode{}
	_values := make([]profTrieValue, len(profile.SampleType))
	for i, name := range profile.SampleType {
		_values[i] = profTrieValue{
			name: fmt.Sprintf("%s:%s", name.Type, name.Unit),
		}
	}
	for _, sample := range profile.Sample {
		parentId := uint64(0)
		for i := len(sample.Location) - 1; i >= 0; i-- {
			loc := sample.Location[i]
			name := "n/a"
			if len(loc.Line) > 0 {
				name = loc.Line[0].Function.Name
			}
			fnId := city.CH64([]byte(name))
			funcs[fnId] = name
			nodeId := getNodeId(parentId, fnId, len(sample.Location)-i)
			node := tree[nodeId]
			if node == nil {
				values := make([]profTrieValue, len(profile.SampleType))
				copy(values, _values)
				node = &profTrieNode{
					parentId: parentId,
					funcId:   fnId,
					nodeId:   nodeId,
					values:   values,
				}

				tree[nodeId] = node
			}
			for j := range node.values {
				node.values[j].total += sample.Value[j]
				if i == 0 {
					node.values[j].self += sample.Value[j]
				}
			}
			parentId = nodeId
		}
	}
	indices := make([]uint64, 0, len(funcs))
	for fnId := range funcs {
		indices = append(indices, fnId)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	var funRes []*model.Function
	for _, fnId := range indices {
		funRes = append(funRes, &model.Function{
			ValueInt64: fnId,
			ValueStr:   funcs[fnId],
		})
	}
	indices = indices[:0]
	for tId := range tree {
		indices = append(indices, tId)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] > indices[j] })
	var tressRes []*profTrieNode
	for _, id := range indices {
		node := tree[id]
		tressRes = append(tressRes, node)

	}
	return funRes, tressRes
}
func getNodeId(parentId uint64, funcId uint64, traceLevel int) uint64 {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], parentId)
	binary.LittleEndian.PutUint64(buf[8:16], funcId)
	if traceLevel > 511 {
		traceLevel = 511
	}
	return city.CH64(buf)>>9 | (uint64(traceLevel) << 55)
}

func NewDecompressor(maxUncompressedSizeBytes int64) *Decompressor {
	return &Decompressor{
		maxUncompressedSizeBytes: maxUncompressedSizeBytes,
		decoders: map[codec]func(r io.Reader) (io.Reader, error){
			Gzip: func(r io.Reader) (io.Reader, error) {
				gr, err := gzip.NewReader(r)
				if err != nil {
					return nil, err
				}
				return gr, nil
			},
		},
	}
}

func processMIMEData(data string) (multipart.File, error) {
	boundary, err := findBoundary(data)
	if err != nil {
		return nil, err
	}
	//buf := new(bytes.Buffer)

	reader := multipart.NewReader(strings.NewReader(data), boundary)
	form, err := reader.ReadForm(10 * 1024 * 1024 * 1024)
	if err != nil {
		return nil, err
	}
	//var part []*multipart.FileHeader
	part, exists := form.File["profile"]
	if !exists || len(part) == 0 {
		return nil, fmt.Errorf("no file found for 'profile' field")
	}
	fh := part[0]
	f, err := fh.Open()
	if err != nil {
		return nil, err
	}

	return f, nil
}

func findBoundary(data string) (string, error) {
	boundaryRegex := regexp.MustCompile(`(?m)^--([A-Za-z0-9'-]+)\r?\n`)
	matches := boundaryRegex.FindStringSubmatch(data)
	if len(matches) > 1 {
		return matches[1], nil
	}
	return "", fmt.Errorf("boundary not found")
}
