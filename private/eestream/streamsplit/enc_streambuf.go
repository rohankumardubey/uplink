package streamsplit

import (
	"io"
	"sync"

	"github.com/zeebo/errs"
	"storj.io/uplink/private/eestream/streambuf"
)

type encryptedStreamBuffer struct {
	sbuf *streambuf.StreamBuffer
	wrc  io.WriteCloser

	mu    sync.Mutex
	plain int64
}

func newEncryptedStreamBuffer(sbuf *streambuf.StreamBuffer, wrc io.WriteCloser) *encryptedStreamBuffer {
	return &encryptedStreamBuffer{
		sbuf: sbuf,
		wrc:  wrc,
	}
}

func (e *encryptedStreamBuffer) Reader() io.Reader     { return e.sbuf.Reader() }
func (e *encryptedStreamBuffer) DoneReading(err error) { e.sbuf.DoneReading(err) }

func (e *encryptedStreamBuffer) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	n, err := e.wrc.Write(p)
	e.plain += int64(n)
	return n, err
}

func (e *encryptedStreamBuffer) PlainSize() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.plain
}

func (e *encryptedStreamBuffer) DoneWriting(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	cerr := e.wrc.Close()
	e.sbuf.DoneWriting(errs.Combine(err, cerr))
}
