package smart_buffer

import (
	"fmt"
	"io"
	"os"
)

// fileBuffer wraps a temporary file for overflow storage when data exceeds RAM limits.
// The file is created lazily on the first write to avoid unnecessary I/O for small responses.
type fileBuffer struct {
	file *os.File
	size int64
}

// newFileBuffer creates a new file buffer. The actual file is not created until
// the first Write call.
func newFileBuffer() *fileBuffer {
	return &fileBuffer{}
}

// Write writes data to the temporary file, creating the file lazily on first write.
// Returns the number of bytes written and any error encountered.
func (b *fileBuffer) Write(p []byte) (int, error) {
	if b.file == nil {
		f, err := os.CreateTemp("", "smartbuffer_*.tmp")
		if err != nil {
			return 0, fmt.Errorf("failed to create temp file: %w", err)
		}
		b.file = f
	}
	n, err := b.file.Write(p)
	b.size += int64(n)
	return n, err
}

// Read reads data from the temporary file at the current file position.
// Returns io.EOF if no file has been created (i.e., no data was ever written).
func (b *fileBuffer) Read(p []byte) (int, error) {
	if b.file == nil {
		return 0, io.EOF
	}
	return b.file.Read(p)
}

// Seek sets the file position for the next Read or Write operation.
// Returns io.EOF if no file has been created.
func (b *fileBuffer) Seek(offset int64, whence int) (int64, error) {
	if b.file == nil {
		return 0, io.EOF
	}
	return b.file.Seek(offset, whence)
}

// Size returns the total number of bytes written to the file.
// Returns 0 if no file has been created.
func (b *fileBuffer) Size() int64 {
	return b.size
}

// Close closes and removes the temporary file.
// Returns any error encountered during removal.
func (b *fileBuffer) Close() error {
	if b.file == nil {
		return nil
	}
	name := b.file.Name()
	b.file.Close()
	return os.Remove(name)
}
