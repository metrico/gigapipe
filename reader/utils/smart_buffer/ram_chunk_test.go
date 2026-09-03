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

func TestRAMChunk_BlocksTileChunkSize(t *testing.T) {
	t.Parallel()

	r := newRAMChunk()
	writeChunks(t, r, chunkSize, 4096)

	if r.Size() != chunkSize {
		t.Errorf("Size() = %d, want %d", r.Size(), chunkSize)
	}

	allocated := 0
	for _, block := range r.blocks {
		allocated += cap(block)
	}
	if allocated != chunkSize {
		t.Errorf("allocated %d bytes, want exactly %d", allocated, chunkSize)
	}

	if _, err := r.Write([]byte("x")); err != ErrBufferFull {
		t.Errorf("Write() on full chunk error = %v, want %v", err, ErrBufferFull)
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
