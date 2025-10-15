package unmarshal

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/go-faster/jx"
	"github.com/metrico/qryn/v4/writer/utils/errors"
)

func jsonParseError(err error) error {
	if err == nil {
		return nil
	}
	return errors.NewUnmarshalError(err)
	//return fmt.Errorf("json parse error: %v", err)
}

type zipkinDecoderV2 struct {
	ctx    *ParserCtx
	onSpan onSpanHandler

	traceId     []byte
	spanId      []byte
	timestampNs int64
	durationNs  int64
	parentId    string
	name        string
	serviceName string
	payload     []byte
	key         []string
	val         []string
}

func (z *zipkinDecoderV2) SetOnEntry(h onSpanHandler) {
	z.onSpan = h
}

func (z *zipkinDecoderV2) Decode() error {
	z.key = make([]string, 10)
	z.val = make([]string, 10)
	dec := jx.Decode(z.ctx.bodyReader, 64*1024)
	return dec.Arr(func(d *jx.Decoder) error {
		z.traceId = nil
		z.spanId = nil
		z.timestampNs = 0
		z.durationNs = 0
		z.parentId = ""
		z.name = ""
		z.serviceName = ""
		z.payload = nil
		z.key = z.key[:0]
		z.val = z.val[:0]
		rawSpan, err := dec.Raw()
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		z.payload = append([]byte{}, rawSpan...)
		return z.decodeSpan(rawSpan)
	})

}

func (z *zipkinDecoderV2) decodeSpan(rawSpan jx.Raw) error {
	dec := jx.DecodeBytes(rawSpan)
	if rawSpan.Type() != jx.Object {
		return errors.New400Error(fmt.Sprintf("span %s is not an object", rawSpan.String()))
	}

	err := dec.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "traceId":
			hexTid, err := dec.StrBytes()
			if err != nil {
				return err
			}
			z.traceId, err = z.decodeHexStr(hexTid, 32)
			return err
		case "id":
			hexSpanId, err := dec.StrBytes()
			if err != nil {
				return err
			}
			z.spanId, err = z.decodeHexStr(hexSpanId, 16)
			return err
		case "parentId":
			parentId, err := d.StrBytes()
			if err != nil {
				return err
			}
			rawParentId, err := z.decodeHexStr(parentId, 16)
			z.parentId = string(rawParentId)
			return err
		case "timestamp":
			var err error
			z.timestampNs, err = z.stringOrInt64(d)
			z.timestampNs *= 1000
			return err
		case "duration":
			val, err := z.stringOrInt64(d)
			z.durationNs = val * 1000
			return err
		case "name":
			var err error
			z.name, err = d.Str()
			z.key = append(z.key, "name")
			z.val = append(z.val, z.name)
			return err
		case "localEndpoint":
			serviceName, err := z.parseEndpoint(d, "local_endpoint_")
			if err != nil {
				return err
			}
			z.serviceName = serviceName
			return nil
		case "remoteEndpoint":
			serviceName, err := z.parseEndpoint(d, "remote_endpoint_")
			if err != nil {
				return err
			}
			if z.serviceName != "" {
				z.serviceName = serviceName
			}
			return nil
		case "tags":
			err := z.parseTags(d)
			return err
		default:
			d.Skip()
		}
		return nil
	})
	if err != nil {
		return errors.NewUnmarshalError(err)
	}
	z.key = append(z.key, "service.name")
	z.val = append(z.val, z.serviceName)
	return z.onSpan(z.traceId, z.spanId, z.timestampNs, z.durationNs, z.parentId,
		z.name, z.serviceName, z.payload, z.key, z.val)
}

func (z *zipkinDecoderV2) stringOrInt64(d *jx.Decoder) (int64, error) {
	next := d.Next()
	switch next {
	case jx.Number:
		return d.Int64()
	case jx.String:
		str, err := d.Str()
		if err != nil {
			return 0, errors.NewUnmarshalError(err)
		}
		return strconv.ParseInt(str, 10, 64)
	}
	return 0, errors.NewUnmarshalError(fmt.Errorf("format not supported"))
}

func (z *zipkinDecoderV2) parseEndpoint(d *jx.Decoder, prefix string) (string, error) {
	serviceName := ""
	err := d.Obj(func(d *jx.Decoder, key string) error {
		switch key {
		case "serviceName":
			val, err := d.Str()
			if err != nil {
				return errors.NewUnmarshalError(err)
			}
			z.key = append(z.key, prefix+"service_name")
			z.val = append(z.val, val)
			serviceName = val
		default:
			return d.Skip()
		}
		return nil
	})
	return serviceName, err
}

func (z *zipkinDecoderV2) parseTags(d *jx.Decoder) error {
	return d.Obj(func(d *jx.Decoder, key string) error {
		tp := d.Next()
		if tp != jx.String {
			return d.Skip()
		}
		z.key = append(z.key, key)
		val, err := d.Str()
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
		z.val = append(z.val, val)
		return nil
	})
}

func (z *zipkinDecoderV2) decodeHexStr(hexStr []byte, leng int) ([]byte, error) {
	if len(hexStr) == 0 {
		return nil, errors.New400Error("hex string is zero")

	}
	if len(hexStr) < leng {
		prefix := make([]byte, leng)
		for i := 0; i < leng; i++ {
			prefix[i] = '0'
		}
		copy(prefix[leng-len(hexStr):], hexStr)
		hexStr = prefix
	}
	hexStr = hexStr[:leng]
	res := make([]byte, leng/2)
	_, err := hex.Decode(res, hexStr)
	if err != nil {
		return nil, errors.NewUnmarshalError(err)
	}
	return res, err
}

type zipkinNDDecoderV2 struct {
	*zipkinDecoderV2
}

func (z *zipkinNDDecoderV2) Decode() error {
	scanner := bufio.NewScanner(z.ctx.bodyReader)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		err := z.decodeSpan(scanner.Bytes())
		if err != nil {
			return errors.NewUnmarshalError(err)
		}
	}
	return nil
}

var UnmarshalZipkinNDJSONV2 = Build(
	withPayloadType(1),
	withSpansParser(func(ctx *ParserCtx) iSpansParser {
		return &zipkinNDDecoderV2{&zipkinDecoderV2{ctx: ctx}}
	}))

var UnmarshalZipkinJSONV2 = Build(
	withPayloadType(1),
	withSpansParser(func(ctx *ParserCtx) iSpansParser { return &zipkinDecoderV2{ctx: ctx} }))
