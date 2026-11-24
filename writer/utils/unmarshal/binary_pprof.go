package unmarshal

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/metrico/qryn/v5/writer/model"
	"github.com/metrico/qryn/v5/writer/utils"
)

// binaryStreamPProfProtoDec extends pProfProtoDec to handle binary/octet-stream content type
type binaryStreamPProfProtoDec struct {
	pProfProtoDec
}

func ns(timestamp uint64) uint64 {
	for timestamp < 1000000000000000000 {
		timestamp *= 10
	}
	return timestamp
}

// Decode implements the specific decoding logic for binary/octet-stream content type
func (b *binaryStreamPProfProtoDec) Decode() error {
	var timestampNs uint64
	var durationNs uint64
	var tags []model.StrStr

	// Parse timestamp and duration from context
	fromValue := b.ctx.ctxMap[utils.ContextKeyFrom]
	start, err := strconv.ParseUint(fromValue, 10, 64)
	if err != nil {
		fmt.Println("start time error:", err.Error())
		return fmt.Errorf("failed to parse start time: %w", err)
	}

	endValue := b.ctx.ctxMap[utils.ContextKeyUntil]
	end, err := strconv.ParseUint(endValue, 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse end time: %w", err)
	}

	// Parse name and extract tags if available
	name := b.ctx.ctxMap[utils.ContextKeyName]
	i := strings.Index(name, "{")
	length := len(name)
	if i < 0 {
		i = length
	} else {
		promqllike := name[i+1 : length-1] // strip {}
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

	// Get buffer from pool for processing
	buf := acquireBuf(b.uncompressedBufPool)
	defer func() {
		releaseBuf(b.uncompressedBufPool, buf)
	}()

	// For binary/octet-stream, we directly read the data without MIME processing
	data, err := io.ReadAll(b.ctx.bodyReader)
	if err != nil {
		fmt.Println("Error reading from reader:", err)
		return err
	}

	// Create a reader from the binary data
	reader := bytes.NewReader(data)
	_, err = io.Copy(buf, reader)
	if err != nil {
		fmt.Println("Error copying data:", err)
		return err
	}
	// Parse the profile data
	ps, err := Parse(buf)
	if err != nil {
		return fmt.Errorf("failed to parse pprof: %w", err)
	}

	// Process the profiles
	for _, profile := range ps {
		if err := emitProfile(b.onProfiles, profile,
			timestampNs, durationNs, name, tags); err != nil {
			return err
		}
	}

	return nil
}

// Create the new unmarshaller for binary/octet-stream content type
var UnmarshalBinaryStreamProfileProtoV2 = Build(
	withStringValueFromCtx(utils.ContextKeyFrom),
	withStringValueFromCtx(utils.ContextKeyName),
	withStringValueFromCtx(utils.ContextKeyUntil),
	withProfileParser(func(ctx *ParserCtx) iProfilesParser {
		dec := &binaryStreamPProfProtoDec{pProfProtoDec{ctx: ctx}}
		return dec
	}))
