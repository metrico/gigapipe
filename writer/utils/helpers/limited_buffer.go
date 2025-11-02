package helpers

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang/snappy"
	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/writer/utils/errors"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/semaphore"
)

var RateLimitingEnable = false

type (
	UUID              [16]byte
	RateLimitedBuffer interface {
		Bytes() []byte
		Write([]byte) (int, error)
		Release()
	}
)

type RateLimitedPooledBuffer struct {
	pool   *RateLimitedPool
	limit  int
	buffer *bytebufferpool.ByteBuffer
}

func (l *RateLimitedPooledBuffer) Write(msg []byte) (int, error) {
	if len(msg)+l.buffer.Len() > l.limit {
		return 0, errors.New400Error("buffer size overflow")
	}
	return l.buffer.Write(msg)
}

func (l *RateLimitedPooledBuffer) Bytes() []byte {
	return l.buffer.Bytes()
}

func (l *RateLimitedPooledBuffer) Release() {
	l.pool.releasePooledBuffer(l)
}

type RateLimitedSliceBuffer struct {
	bytes []byte
	pool  *RateLimitedPool
}

func (l *RateLimitedSliceBuffer) Write(msg []byte) (int, error) {
	return 0, nil
}

func (l *RateLimitedSliceBuffer) Bytes() []byte {
	return l.bytes
}

func (l *RateLimitedSliceBuffer) Release() {
	if l.pool != nil {
		l.pool.releaseSlice(l)
	}
}

type RateLimitedPool struct {
	limit       int
	rateLimiter *semaphore.Weighted
}

func (r *RateLimitedPool) acquirePooledBuffer(limit int) (RateLimitedBuffer, error) {
	if limit > r.limit {
		return nil, fmt.Errorf("limit too big")
	}
	if RateLimitingEnable {
		to, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := r.rateLimiter.Acquire(to, int64(limit))
		if err != nil {
			return nil, err
		}
	}
	return &RateLimitedPooledBuffer{
		pool:   r,
		limit:  limit,
		buffer: bytebufferpool.Get(),
	}, nil
}

func (r *RateLimitedPool) releasePooledBuffer(buffer *RateLimitedPooledBuffer) {
	if RateLimitingEnable {
		r.rateLimiter.Release(int64(buffer.limit))
	}
	// bytebufferpool.Put(buffer.buffer)
}

func (r *RateLimitedPool) acquireSlice(size int) (RateLimitedBuffer, error) {
	if size > r.limit {
		return nil, errors.New400Error("size too big")
	}
	if RateLimitingEnable {
		to, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := r.rateLimiter.Acquire(to, int64(size))
		if err != nil {
			return nil, err
		}
	}
	return &RateLimitedSliceBuffer{
		bytes: make([]byte, size),
		pool:  r,
	}, nil
}

func (r *RateLimitedPool) releaseSlice(buffer *RateLimitedSliceBuffer) {
	// r.rateLimiter.Release(int64(len(buffer.bytes)))
}

var requestPool = RateLimitedPool{
	limit:       50 * 1024 * 1024,
	rateLimiter: semaphore.NewWeighted(50 * 1024 * 1024),
}

var pbPool = RateLimitedPool{
	limit:       50 * 1024 * 1024,
	rateLimiter: semaphore.NewWeighted(50 * 1024 * 1024),
}

func getPayloadBuffer(ctx *fiber.Ctx) (RateLimitedBuffer, error) {
	var ctxLen int
	if ctx.Get("content-length", "") == "" {
		return nil, errors.New400Error("content-length is required")
		// return nil, util.NewCLokiWriterError(400, "content-length is required")
	} else {
		ctxLen, _ = strconv.Atoi(ctx.Get("content-length", ""))
	}
	buf, err := requestPool.acquirePooledBuffer(ctxLen)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(buf, ctx.Context().RequestBodyStream())
	if err != nil {
		buf.Release()
		return nil, err
	}
	return buf, nil
}

func decompressPayload(buf RateLimitedBuffer) (RateLimitedBuffer, error) {
	decompSize, err := snappy.DecodedLen(buf.Bytes())
	if err != nil {
		return nil, err
	}
	if decompSize > pbPool.limit {
		return nil, errors.New400Error("decompressed request too long")
		// return nil, util.NewCLokiWriterError(400, "decompressed request too long")
	}
	slice, err := pbPool.acquireSlice(decompSize)
	if err != nil {
		return nil, err
	}
	_, err = snappy.Decode(slice.Bytes(), buf.Bytes())
	if err != nil {
		slice.Release()
		logger.Error(err)
		return nil, errors.New400Error("request decompress error")
		// return nil, util.NewCLokiWriterError(400, "request decompress error")
	}
	return slice, nil
}

func GetRawBody(ctx *fiber.Ctx) (RateLimitedBuffer, error) {
	buf, err := getPayloadBuffer(ctx)
	if err != nil {
		return nil, err
	}
	if ctx.Get("content-encoding") == "gzip" {
		defer buf.Release()
		reader := bytes.NewReader([]byte(buf.Bytes()))
		gzreader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		buf2 := bytes.Buffer{}
		_, err = io.Copy(&buf2, gzreader)
		if err != nil {
			return nil, err
		}
		return &RateLimitedSliceBuffer{bytes: buf2.Bytes()}, nil
	}
	if ctx.Get("content-type", "") != "application/x-protobuf" {
		return buf, nil
	}
	defer buf.Release()
	// t1 := time.Now().UnixNano()
	slice, err := decompressPayload(buf)
	// stat.AddSentMetrics("Decompression time", time.Now().UnixNano()-t1)
	return slice, err
}

func GetRawCompressedBody(ctx *fiber.Ctx) (RateLimitedBuffer, error) {
	return getPayloadBuffer(ctx)
}

func GetRawPB(ctx *fiber.Ctx) (RateLimitedBuffer, error) {
	buf, err := getPayloadBuffer(ctx)
	if err != nil {
		return nil, err
	}
	defer buf.Release()
	return decompressPayload(buf)
}

func SetGlobalLimit(limit int) {
	requestPool.limit = limit / 2
	requestPool.rateLimiter = semaphore.NewWeighted(int64(limit / 2))
	pbPool.limit = limit / 2
	pbPool.rateLimiter = semaphore.NewWeighted(int64(limit / 2))
}
