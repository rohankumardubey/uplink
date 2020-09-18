package streambuf

import (
	"io"
	"sync"
)

// StreamBuffer lets one write to some Buffer and lazily create readers from it.
type StreamBuffer struct {
	mu     sync.Mutex
	cursor *Cursor
	buffer Buffer
	wrote  int64
}

// NewStreamBuffer constructs a StreamBuffer using the underlying Buffer and allowing
// the writer to write writeAhead extra bytes in front of the most advanced reader.
func NewStreamBuffer(buffer Buffer, writeAhead int64) *StreamBuffer {
	return &StreamBuffer{
		cursor: NewCursor(writeAhead),
		buffer: buffer,
	}
}

// DoneReading signals to the StreamBuffer that no more Read calls are coming and
// that Write calls should return the provided error.
func (w *StreamBuffer) DoneReading(err error) {
	if w.cursor.DoneReading(err) {
		w.buffer.Close()
	}
}

// DoneWriting signals to the StreamBuffer that no more Write calls are coming and
// that Read calls should return the provided error.
func (w *StreamBuffer) DoneWriting(err error) {
	if w.cursor.DoneWriting(err) {
		w.buffer.Close()
	}
}

// Write appends the bytes in p to the StreamBuffer.
func (w *StreamBuffer) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for len(p) > 0 {
		m, ok, err := w.cursor.WaitWrite(w.wrote + int64(len(p)))
		if err != nil {
			return n, err
		} else if !ok {
			return n, nil
		}

		var nn int
		nn, err = w.buffer.Write(p[:m-w.wrote])
		n += nn
		p = p[nn:]
		w.wrote += int64(nn)
		w.cursor.WroteTo(w.wrote)

		if err != nil {
			w.cursor.DoneWriting(err)
			return n, err
		}
	}
	return n, nil
}

// streamBufferReader is the concrete implementation returned from the
// StreamBuffer.Reader method.
type streamBufferReader struct {
	mu     sync.Mutex
	cursor *Cursor
	buffer Buffer
	read   int64
}

// Reader returns a fresh io.Reader that can be used to read all of the previously
// and future written bytes to the StreamBuffer.
func (w *StreamBuffer) Reader() io.Reader {
	return &streamBufferReader{
		cursor: w.cursor,
		buffer: w.buffer,
	}
}

// Read reads bytes into p, waiting for writes to happen if needed. It returns
// 0, io.EOF when the end of the stream has been reached.
func (w *streamBufferReader) Read(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	m, ok, err := w.cursor.WaitRead(w.read + int64(len(p)))
	if err != nil {
		return 0, err
	} else if m == w.read {
		return 0, io.EOF
	}

	n, err = w.buffer.ReadAt(p[:m-w.read], w.read)
	w.read += int64(n)
	w.cursor.ReadTo(w.read)

	if err != nil {
		return n, err
	} else if !ok {
		return n, io.EOF
	}
	return n, nil
}
