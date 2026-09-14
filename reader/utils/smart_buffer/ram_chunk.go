package smart_buffer

import (
	"errors"
	"io"
	"math/bits"
	"sync"
)

// ErrBufferFull is returned when the RAM chunk cannot accept more data.
var ErrBufferFull = errors.New("buffer is full")

const (
	// chunkSize defines the maximum size of the RAM buffer (5MB).
	chunkSize = 5 * 1000 * 1000 // 5MB
	// initialBlockSize is the size of the first block, small enough that a
	// few-hundred-byte response costs a single small allocation.
	initialBlockSize = 8 * 1024
	// maxBlockSize caps the block size, so a full chunk is a couple of dozen
	// blocks rather than a couple of hundred writes on the spill path.
	maxBlockSize = 256 * 1024
	blockClasses = 6 // 8KB..256KB
)

// blockPools holds spare blocks of every ladder size. A response reuses the
// memory of the ones before it instead of faulting in fresh pages.
var blockPools [blockClasses]sync.Pool

func blockClass(size int) int {
	return bits.Len(uint(size/initialBlockSize)) - 1
}

func getBlock(size int) []byte {
	if b, _ := blockPools[blockClass(size)].Get().(*[]byte); b != nil {
		return (*b)[:0]
	}
	return make([]byte, 0, size)
}

func putBlock(b []byte) {
	b = b[:0]
	blockPools[blockClass(cap(b))].Put(&b)
}

// ramChunk accumulates up to chunkSize bytes in a list of separately allocated
// blocks. Blocks are never reallocated or copied, so the chunk holds only what
// is actually written: a growing single slice would copy everything accumulated
// so far on each growth step. Blocks are reused after Clear and returned to the
// pool by Release.
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
			r.blocks = append(r.blocks, getBlock(r.nextBlockSize()))
		}
		block := r.blocks[r.cur]
		if len(block) == cap(block) {
			r.cur++
			continue
		}
		n := min(len(p), cap(block)-len(block), chunkSize-r.size)
		r.blocks[r.cur] = append(block, p[:n]...)
		p = p[n:]
		r.size += n
		written += n
	}
	return written, nil
}

// nextBlockSize doubles the previous block size up to maxBlockSize.
func (r *ramChunk) nextBlockSize() int {
	if r.cur == 0 {
		return initialBlockSize
	}
	return min(cap(r.blocks[r.cur-1])*2, maxBlockSize)
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
		if len(block) == 0 {
			break
		}
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

// Release returns the blocks to the pool. The chunk is empty afterwards and can
// be written to again.
func (r *ramChunk) Release() {
	for _, block := range r.blocks {
		putBlock(block)
	}
	r.blocks = nil
	r.cur = 0
	r.size = 0
	r.readBlock = 0
	r.readOff = 0
}

// Size returns the current number of bytes stored in the chunk.
func (r *ramChunk) Size() int {
	return r.size
}
