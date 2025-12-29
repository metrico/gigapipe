package smart_buffer

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestSmartBuffer_SmallData(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{
			name:    "1KB data stays in RAM",
			size:    1024,
			wantErr: false,
		},
		{
			name:    "100KB data stays in RAM",
			size:    100 * 1024,
			wantErr: false,
		},
		{
			name:    "1MB data stays in RAM",
			size:    1024 * 1024,
			wantErr: false,
		},
		{
			name:    "4.9MB data stays in RAM",
			size:    4900000,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create test data
			input := make([]byte, tt.size)
			if _, err := rand.Read(input); err != nil {
				t.Fatalf("failed to generate random data: %v", err)
			}

			// Write to buffer
			buf := New()
			defer buf.Close()

			n, err := buf.Write(input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				t.Logf("error details: %v", err)
				return
			}
			if n != tt.size {
				t.Errorf("Write() wrote %d bytes, want %d", n, tt.size)
			}

			// Verify buffer size
			if buf.Size() != int64(tt.size) {
				t.Errorf("Size() = %d, want %d", buf.Size(), tt.size)
			}

			// Verify file was not created
			if buf.file.Size() > 0 {
				t.Errorf("file buffer should be empty for data < 5MB, got size %d", buf.file.Size())
			}

			// Read back and verify
			output := make([]byte, tt.size)
			n, err = io.ReadFull(buf, output)
			if err != nil {
				t.Errorf("Read() error = %v", err)
			}
			if n != tt.size {
				t.Errorf("Read() read %d bytes, want %d", n, tt.size)
			}

			if !bytes.Equal(input, output) {
				t.Errorf("output does not match input")
			}
		})
	}
}

func TestSmartBuffer_MediumData(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{
			name:    "6MB data uses file",
			size:    6 * 1000 * 1000,
			wantErr: false,
		},
		{
			name:    "10MB data uses file",
			size:    10 * 1000 * 1000,
			wantErr: false,
		},
		{
			name:    "50MB data uses file",
			size:    50 * 1000 * 1000,
			wantErr: false,
		},
		{
			name:    "99MB data uses file",
			size:    99 * 1000 * 1000,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create test data
			input := make([]byte, tt.size)
			if _, err := rand.Read(input); err != nil {
				t.Fatalf("failed to generate random data: %v", err)
			}

			// Write to buffer
			buf := New()
			defer buf.Close()

			n, err := buf.Write(input)
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				t.Logf("error details: %v", err)
				return
			}

			// Verify buffer size (more reliable than write return value)
			if buf.Size() != int64(tt.size) {
				t.Errorf("Size() = %d, want %d", buf.Size(), tt.size)
			}

			// Verify file was created
			if buf.file.Size() == 0 {
				t.Errorf("file buffer should be used for data >= 5MB")
			}

			// Read back and verify
			output := make([]byte, tt.size)
			n, err = io.ReadFull(buf, output)
			if err != nil {
				t.Errorf("Read() error = %v", err)
			}
			if n != tt.size {
				t.Errorf("Read() read %d bytes, want %d", n, tt.size)
			}

			if !bytes.Equal(input, output) {
				t.Errorf("output does not match input")
			}
		})
	}
}

func TestSmartBuffer_SizeLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{
			name:    "100MB exactly at limit",
			size:    100 * 1000 * 1000,
			wantErr: false,
		},
		{
			name:    "100MB + 1 byte exceeds limit",
			size:    100*1000*1000 + 1,
			wantErr: true,
		},
		{
			name:    "150MB exceeds limit",
			size:    150 * 1000 * 1000,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := New()
			defer buf.Close()

			// Write data in chunks to avoid huge memory allocation
			chunkSize := 10 * 1000 * 1000 // 10MB chunks
			written := 0
			var lastErr error

			for written < tt.size {
				toWrite := chunkSize
				if written+toWrite > tt.size {
					toWrite = tt.size - written
				}

				chunk := make([]byte, toWrite)
				_, err := buf.Write(chunk)
				if err != nil {
					lastErr = err
					break
				}
				written += toWrite
			}

			if (lastErr != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", lastErr, tt.wantErr)
			}
			if lastErr != nil {
				t.Logf("error details: %v", lastErr)
			}
		})
	}
}

func TestSmartBuffer_WriteAfterRead(t *testing.T) {
	t.Parallel()

	buf := New()
	defer buf.Close()

	// Write some data
	data := []byte("test data")
	_, err := buf.Write(data)
	if err != nil {
		t.Fatalf("initial Write() failed: %v", err)
	}

	// Read to trigger read mode
	readBuf := make([]byte, len(data))
	_, err = buf.Read(readBuf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read() failed: %v", err)
	}

	// Try to write after reading
	_, err = buf.Write([]byte("more data"))
	if err != ErrWriteAfterRead {
		t.Errorf("Write() after Read() error = %v, want %v", err, ErrWriteAfterRead)
	}
}

func TestSmartBuffer_MultipleWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writeSizes []int
		wantErr    bool
	}{
		{
			name:       "multiple small writes",
			writeSizes: []int{1000, 2000, 3000, 4000},
			wantErr:    false,
		},
		{
			name:       "writes crossing 5MB boundary",
			writeSizes: []int{2 * 1000 * 1000, 2 * 1000 * 1000, 2 * 1000 * 1000},
			wantErr:    false,
		},
		{
			name: "many small writes totaling 10MB",
			writeSizes: func() []int {
				sizes := make([]int, 100)
				for i := range sizes {
					sizes[i] = 100000 // 100KB each
				}
				return sizes
			}(),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := New()
			defer buf.Close()

			var allData []byte
			for _, size := range tt.writeSizes {
				chunk := make([]byte, size)
				if _, err := rand.Read(chunk); err != nil {
					t.Fatalf("failed to generate random data: %v", err)
				}
				allData = append(allData, chunk...)

				_, err := buf.Write(chunk)
				if (err != nil) != tt.wantErr {
					t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				}
				if err != nil {
					t.Logf("error details: %v", err)
					return
				}
			}

			// Verify total size
			expectedSize := 0
			for _, s := range tt.writeSizes {
				expectedSize += s
			}
			if buf.Size() != int64(expectedSize) {
				t.Errorf("Size() = %d, want %d", buf.Size(), expectedSize)
			}

			// Read back and verify
			output := make([]byte, expectedSize)
			n, err := io.ReadFull(buf, output)
			if err != nil {
				t.Errorf("Read() error = %v", err)
			}
			if n != expectedSize {
				t.Errorf("Read() read %d bytes, want %d", n, expectedSize)
			}

			if !bytes.Equal(allData, output) {
				t.Errorf("output does not match input")
			}
		})
	}
}

func TestSmartBuffer_EmptyBuffer(t *testing.T) {
	t.Parallel()

	buf := New()
	defer buf.Close()

	// Read from empty buffer
	data := make([]byte, 10)
	n, err := buf.Read(data)
	if err != io.EOF {
		t.Errorf("Read() from empty buffer error = %v, want %v", err, io.EOF)
	}
	if n != 0 {
		t.Errorf("Read() from empty buffer read %d bytes, want 0", n)
	}

	// Verify size
	if buf.Size() != 0 {
		t.Errorf("Size() = %d, want 0", buf.Size())
	}
}
