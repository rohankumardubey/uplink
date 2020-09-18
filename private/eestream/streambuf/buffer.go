package streambuf

import (
	"io"
	"sync/atomic"
)

// Buffer is a backing store of bytes for a stream buffer.
type Buffer interface {
	io.Writer
	io.ReaderAt
	io.Closer
}

func NewMemoryBuffer(cap int) *MemoryBuffer {
	buf := make([]byte, 0, cap)
	mbuf := new(MemoryBuffer)
	mbuf.buf.Store(buf)
	return mbuf
}

// MemoryBuffer implements the Buffer interface backed by a slice.
type MemoryBuffer struct {
	buf atomic.Value // []byte
}

func (u *MemoryBuffer) Reset() {
	buf, _ := u.buf.Load().([]byte)
	u.buf.Store(buf[:0])
}

// Write appends the data to the buffer.
func (u *MemoryBuffer) Write(p []byte) (n int, err error) {
	buf, _ := u.buf.Load().([]byte)
	buf = append(buf, p...)
	u.buf.Store(buf)
	return len(p), nil
}

// ReadAt reads into the provided buffer p starting at off.
func (u *MemoryBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	buf, _ := u.buf.Load().([]byte)
	if off >= int64(len(buf)) {
		return 0, io.EOF
	}
	n = copy(p, buf[off:])
	return n, nil
}

// Close is a no-op for a memory buffer.
func (u *MemoryBuffer) Close() error {
	return nil
}
