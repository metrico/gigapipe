package unmarshal

import (
	"bufio"
	"fmt"
	"time"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/writer/model"
	"github.com/metrico/qryn/writer/utils"
	"github.com/metrico/qryn/writer/utils/errors"
)

type datadogCFRequestDec struct {
	ctx *ParserCtx

	DDSource     string
	ScriptName   string
	Outcome      string
	EventType    string
	TsNs         int64
	ActionResult *bool
	ActionType   string
	ActorType    string
	ResourceType string

	onEntries onEntriesHandler
}

func (d *datadogCFRequestDec) Decode() error {
	scanner := bufio.NewScanner(d.ctx.bodyReader)
	scanner.Split(bufio.ScanLines)

	d.DDSource = d.ctx.ctxMap[utils.ContextKeyDDSource]
	for scanner.Scan() {
		bytes := scanner.Bytes()
		err := d.DecodeLine(bytes)
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		t := time.Now()
		if d.TsNs != 0 {
			t = time.Unix(d.TsNs/1000000000, d.TsNs%1000000000)
		}
		err = d.onEntries(d.GetLabels(), []int64{t.UnixNano()}, []string{scanner.Text()}, []float64{0},
			[]uint8{model.SAMPLE_TYPE_LOG})
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *datadogCFRequestDec) SetOnEntries(h onEntriesHandler) {
	d.onEntries = h
}

func (d *datadogCFRequestDec) DecodeLine(line []byte) error {
	d.ScriptName = ""
	d.Outcome = ""
	d.EventType = ""
	d.TsNs = 0
	d.ActionResult = nil
	d.ActionType = ""
	d.ActorType = ""
	d.ResourceType = ""
	dec := jx.DecodeBytes(line)
	return dec.Obj(d.decodeRootObj)

}

func (d *datadogCFRequestDec) decodeRootObj(dec *jx.Decoder, key string) error {
	var err error
	switch key {
	case "EventType":
		d.EventType, err = dec.Str()
		return err
	case "Outcome":
		d.Outcome, err = dec.Str()
		return err
	case "ScriptName":
		d.ScriptName, err = dec.Str()
		return err
	case "EventTimestampMs":
		tp := dec.Next()
		switch tp {
		case jx.Number:
			d.TsNs, err = dec.Int64()
			d.TsNs *= 1000000
			return err
		}
	case "When":
		tp := dec.Next()
		switch tp {
		case jx.Number:
			d.TsNs, err = dec.Int64()
			return err
		}
	case "ActionResult":
		tp := dec.Next()
		switch tp {
		case jx.Bool:
			actRes := false
			actRes, err = dec.Bool()
			d.ActionResult = &actRes
		}
		return err
	case "ActionType":
		d.ActionType, err = dec.Str()
		return err
	case "ActorType":
		d.ActorType, err = dec.Str()
		return err
	case "ResourceType":
		d.ResourceType, err = dec.Str()
		return err
	}
	return dec.Skip()
}

func (d *datadogCFRequestDec) GetLabels() [][]string {
	strActResult := ""
	if d.ActionResult != nil {
		strActResult = fmt.Sprintf("%v", *d.ActionResult)
	}
	var labels [][]string
	for _, label := range [][]string{
		{"ddsource", d.DDSource},
		{"ScriptName", d.ScriptName},
		{"Outcome", d.Outcome},
		{"EventType", d.EventType},
		{"ActionResult", strActResult},
		{"ActionType", d.ActionType},
		{"ActorType", d.ActorType},
		{"ResourceType", d.ResourceType},
	} {
		if label[1] != "" {
			labels = append(labels, label)
		}
	}
	return labels
}

var UnmarshallDatadogCFJSONV2 = Build(
	withStringValueFromCtx(utils.ContextKeyDDSource),
	withLogsParser(func(ctx *ParserCtx) iLogsParser {
		return &datadogCFRequestDec{ctx: ctx}
	}))
