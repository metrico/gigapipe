package smart_buffer

import (
	"errors"
	"fmt"
	"io"
)

const (
	// maxTotalSize is the maximum allowed buffer size (100MB).
	maxTotalSize = 100 * 1000 * 1000 // 100MB
)

var (
	// ErrSizeExceeded is returned when a write would exceed the 100MB limit.
	ErrSizeExceeded = errors.New("maximum buffer size exceeded")
	// ErrWriteAfterRead is returned when attempting to write after reading has started.
	ErrWriteAfterRead = errors.New("cannot write after read has started")
)

// SmartBuffer stores data in RAM up to 5MB,
// then automatically overflows to a temporary file on disk. It enforces
// a 100MB maximum size limit and supports streaming via io.Reader.
// The buffer transitions from write mode to read mode on the first Read() call.
// After reading starts, writes are no longer allowed.
type SmartBuffer struct {
	chunk       *ramChunk
	file        *fileBuffer
	size        int64
	readStarted bool
	readPos     int64
}

// New creates a new smart buffer instance.
func New() *SmartBuffer {
	return &SmartBuffer{
		chunk: newRAMChunk(),
		file:  newFileBuffer(),
	}
}

// Write implements io.Writer. It buffers data in RAM up to 5MB, then automatically
// flushes to a temporary file. Returns ErrSizeExceeded if the total size would
// exceed 100MB. Returns ErrWriteAfterRead if called after reading has started.
func (b *SmartBuffer) Write(data []byte) (int, error) {
	if b.readStarted {
		return 0, ErrWriteAfterRead
	}

	if b.size+int64(len(data)) > maxTotalSize {
		return 0, ErrSizeExceeded
	}

	written, err := b.chunk.Write(data)
	b.size += int64(written)

	if err == nil {
		return written, nil
	}
	if err != ErrBufferFull {
		return 0, fmt.Errorf("failed to write to RAM chunk: %w", err)
	}

	if err := b.chunk.Flush(b.file); err != nil {
		return 0, fmt.Errorf("failed to flush RAM chunk to file: %w", err)
	}

	return b.Write(data[written:])
}

// Read implements io.Reader. On the first call, it finalizes the buffer by flushing
// any remaining RAM data to the file (if file exists) and seeks to the beginning.
// Subsequent calls stream data from either RAM (if < 5MB total) or the temp file.
// Returns io.EOF when all data has been read.
func (b *SmartBuffer) Read(p []byte) (n int, err error) {
	if !b.readStarted {
		b.readStarted = true

		if b.file.Size() > 0 {
			if b.chunk.Size() > 0 {
				if err := b.chunk.Flush(b.file); err != nil {
					return 0, fmt.Errorf("failed to flush RAM chunk to file: %w", err)
				}
			}
			if _, err := b.file.Seek(0, io.SeekStart); err != nil {
				return 0, fmt.Errorf("failed to seek file buffer to start: %w", err)
			}
		}
	}

	if b.file.Size() == 0 {
		bytes := b.chunk.Bytes()
		if b.readPos >= int64(len(bytes)) {
			return 0, io.EOF
		}
		n := copy(p, bytes[b.readPos:])
		b.readPos += int64(n)
		if b.readPos >= int64(len(bytes)) {
			return n, io.EOF
		}
		return n, nil
	}

	return b.file.Read(p)
}

// Close cleans up resources by closing and removing the temporary file if one was created.
func (b *SmartBuffer) Close() error {
	return b.file.Close()
}

// Size returns the total number of bytes written to the buffer.
func (b *SmartBuffer) Size() int64 {
	return b.size
}
