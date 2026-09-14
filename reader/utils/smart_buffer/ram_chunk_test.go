package smart_buffer

import (
	"bytes"
	"io"
	"testing"
)

func writeChunks(t *testing.T, r *ramChunk, total, per int) {
	t.Helper()
	data := make([]byte, per)
	for written := 0; written < total; {
		n, err := r.Write(data[:min(per, total-written)])
		if err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		written += n
	}
}

func TestRAMChunk_StopsAtChunkSize(t *testing.T) {
	t.Parallel()

	r := newRAMChunk()
	writeChunks(t, r, chunkSize, 4096)

	if r.Size() != chunkSize {
		t.Errorf("Size() = %d, want %d", r.Size(), chunkSize)
	}

	allocated := 0
	for _, block := range r.blocks {
		if cap(block) > maxBlockSize {
			t.Errorf("block capacity %d exceeds %d", cap(block), maxBlockSize)
		}
		allocated += cap(block)
	}
	if allocated < chunkSize || allocated >= chunkSize+maxBlockSize {
		t.Errorf("allocated %d bytes, want [%d, %d)", allocated, chunkSize, chunkSize+maxBlockSize)
	}

	if _, err := r.Write([]byte("x")); err != ErrBufferFull {
		t.Errorf("Write() on full chunk error = %v, want %v", err, ErrBufferFull)
	}
}

func TestRAMChunk_ReleaseReusesBlocks(t *testing.T) {
	t.Parallel()

	r := newRAMChunk()
	writeChunks(t, r, 64*1024, 4096)
	first := make([][]byte, len(r.blocks))
	copy(first, r.blocks)
	r.Release()

	if r.Size() != 0 {
		t.Errorf("Size() after Release = %d, want 0", r.Size())
	}

	r2 := newRAMChunk()
	writeChunks(t, r2, 64*1024, 4096)
	reused := 0
	for i, block := range r2.blocks {
		if i < len(first) && &block[:1][0] == &first[i][:1][0] {
			reused++
		}
	}
	if reused == 0 {
		t.Error("no block was taken from the pool")
	}
}

func TestRAMChunk_ReusesBlocksAfterFlush(t *testing.T) {
	t.Parallel()

	r := newRAMChunk()
	writeChunks(t, r, 256*1024, 4096)
	blocks := len(r.blocks)

	var sink bytes.Buffer
	if err := r.Flush(&sink); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if sink.Len() != 256*1024 {
		t.Errorf("flushed %d bytes, want %d", sink.Len(), 256*1024)
	}

	writeChunks(t, r, 256*1024, 4096)
	if len(r.blocks) != blocks {
		t.Errorf("refill allocated %d blocks, want %d reused", len(r.blocks), blocks)
	}
}

func TestRAMChunk_ReadAcrossBlocks(t *testing.T) {
	t.Parallel()

	input := bytes.Repeat([]byte("gigapipe"), 40000) // 320KB, spans many blocks
	r := newRAMChunk()
	if _, err := r.Write(input); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !bytes.Equal(input, output) {
		t.Errorf("output does not match input")
	}
}

func BenchmarkSmartBuffer(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"200B", 200},
		{"64KB", 64 * 1024},
		{"1MB", 1024 * 1024},
		{"5MB", chunkSize},
	}
	data := make([]byte, 4096)

	for _, tt := range sizes {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				buf := New()
				for written := 0; written < tt.size; written += len(data) {
					if _, err := buf.Write(data[:min(len(data), tt.size-written)]); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := io.Copy(io.Discard, buf); err != nil {
					b.Fatal(err)
				}
				buf.Close()
			}
		})
	}
}
