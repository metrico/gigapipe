package smart_buffer

import (
	"errors"
	"io"
)

// ErrBufferFull is returned when the RAM chunk cannot accept more data.
var ErrBufferFull = errors.New("buffer is full")

const (
	// chunkSize defines the maximum size of the RAM buffer (5MB).
	chunkSize = 5 * 1000 * 1000 // 5MB
	// initialBlockSize is the size of the first block, small enough that a
	// few-hundred-byte response costs a single small allocation.
	initialBlockSize = 8 * 1024
	// maxBlockSize caps the block size at Go's largest size class, so every
	// block stays on the small-object allocation path.
	maxBlockSize = 32 * 1024
)

// ramChunk accumulates up to chunkSize bytes in a list of separately allocated
// blocks. Blocks are never reallocated or copied, so the chunk allocates only
// what is actually written: a growing single slice would copy everything
// accumulated so far on each growth step. Blocks are reused after Clear.
// It returns ErrBufferFull when it cannot accept more data.
type ramChunk struct {
	blocks    [][]byte
	cur       int // block currently being filled
	size      int
	readBlock int
	readOff   int
}

// newRAMChunk creates a new RAM chunk. No memory is reserved until the first write.
func newRAMChunk() *ramChunk {
	return &ramChunk{}
}

// Write writes data to the RAM chunk, writing as much as will fit.
// Returns the number of bytes written and ErrBufferFull if not all data could be written.
func (r *ramChunk) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if r.size >= chunkSize {
			return written, ErrBufferFull
		}
		if r.cur == len(r.blocks) {
			r.blocks = append(r.blocks, make([]byte, 0, r.nextBlockSize()))
		}
		block := r.blocks[r.cur]
		if len(block) == cap(block) {
			r.cur++
			continue
		}
		n := min(len(p), cap(block)-len(block))
		r.blocks[r.cur] = append(block, p[:n]...)
		p = p[n:]
		r.size += n
		written += n
	}
	return written, nil
}

// nextBlockSize doubles the previous block size up to maxBlockSize, clamped to
// the room left in the chunk so the blocks tile chunkSize exactly.
func (r *ramChunk) nextBlockSize() int {
	size := initialBlockSize
	if r.cur > 0 {
		size = min(cap(r.blocks[r.cur-1])*2, maxBlockSize)
	}
	return min(size, chunkSize-r.size)
}

// Read implements io.Reader over the accumulated blocks.
// Returns io.EOF once all data has been read.
func (r *ramChunk) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) && r.readBlock < len(r.blocks) {
		block := r.blocks[r.readBlock]
		if r.readOff == len(block) {
			r.readBlock++
			r.readOff = 0
			continue
		}
		copied := copy(p[n:], block[r.readOff:])
		r.readOff += copied
		n += copied
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

// Flush writes all accumulated data in the chunk to the provided writer
// and clears the chunk for reuse. If the chunk is empty, this is a no-op.
func (r *ramChunk) Flush(w io.Writer) error {
	if r.size == 0 {
		return nil
	}
	for _, block := range r.blocks {
		if _, err := w.Write(block); err != nil {
			return err
		}
	}
	r.Clear()
	return nil
}

// Clear resets the chunk to empty state, keeping the blocks for reuse.
func (r *ramChunk) Clear() {
	for i := range r.blocks {
		r.blocks[i] = r.blocks[i][:0]
	}
	r.cur = 0
	r.size = 0
	r.readBlock = 0
	r.readOff = 0
}

// Size returns the current number of bytes stored in the chunk.
func (r *ramChunk) Size() int {
	return r.size
}
