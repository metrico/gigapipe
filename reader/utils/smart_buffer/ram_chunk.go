package smart_buffer

import (
	"errors"
	"io"
)

// ErrBufferFull is returned when the RAM chunk cannot accept more data.
var ErrBufferFull = errors.New("buffer is full")

// chunkSize defines the maximum size of the RAM buffer (5MB).
const chunkSize = 5 * 1000 * 1000 // 5MB

// ramChunk is a fixed-size RAM buffer that accumulates data up to chunkSize.
// It returns ErrBufferFull when it cannot accept more data.
type ramChunk struct {
	data []byte
	size int
}

// newRAMChunk creates a new RAM chunk with pre-allocated capacity.
func newRAMChunk() *ramChunk {
	return &ramChunk{
		data: make([]byte, 0, chunkSize),
	}
}

// Write writes data to the RAM chunk, writing as much as will fit.
// Returns the number of bytes written and ErrBufferFull if not all data could be written.
func (r *ramChunk) Write(p []byte) (int, error) {
	available := chunkSize - r.size
	if available == 0 {
		return 0, ErrBufferFull
	}

	written := min(len(p), available)

	r.data = append(r.data, p[:written]...)
	r.size += written

	if written < len(p) {
		return written, ErrBufferFull
	}

	return written, nil
}

// Flush writes all accumulated data in the chunk to the provided writer
// and clears the chunk for reuse. If the chunk is empty, this is a no-op.
func (r *ramChunk) Flush(w io.Writer) error {
	if r.size == 0 {
		return nil
	}
	if _, err := w.Write(r.data); err != nil {
		return err
	}
	r.Clear()
	return nil
}

// Clear resets the chunk to empty state.
func (r *ramChunk) Clear() {
	r.data = r.data[:0]
	r.size = 0
}

// Bytes returns the current data in the chunk.
// The returned slice should not be modified.
func (r *ramChunk) Bytes() []byte {
	return r.data
}

// Size returns the current number of bytes stored in the chunk.
func (r *ramChunk) Size() int {
	return r.size
}
