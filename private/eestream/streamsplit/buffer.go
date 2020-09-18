package streamsplit

import (
	"os"
	"sync"

	"github.com/zeebo/errs"

	"storj.io/uplink/private/eestream/streambuf"
)

//
// pooled memory buffer
//

var memPool = sync.Pool{
	New: func() interface{} {
		return new(streambuf.MemoryBuffer)
	},
}

type PooledMemoryBuffer struct {
	mu  sync.Mutex
	buf *streambuf.MemoryBuffer
}

func (m *PooledMemoryBuffer) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf == nil {
		return 0, errs.New("already closed")
	}
	return m.buf.ReadAt(p, off)
}

func (m *PooledMemoryBuffer) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf == nil {
		return 0, errs.New("already closed")
	}
	return m.buf.Write(p)
}

func (m *PooledMemoryBuffer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf != nil {
		m.buf.Reset()
		memPool.Put(m.buf)
		m.buf = nil
	}

	return nil // Close is a noop for a streambuf.MemoryBuffer
}

func NewPooledMemoryBuffer() streambuf.Buffer {
	buf := memPool.Get().(*streambuf.MemoryBuffer)
	return &PooledMemoryBuffer{buf: buf}
}

//
// file buffer
//

type fileBuffer os.File

func (b *fileBuffer) file() *os.File {
	return (*os.File)(b)
}

func (b *fileBuffer) ReadAt(p []byte, off int64) (int, error) {
	return b.file().ReadAt(p, off)
}

func (b *fileBuffer) Write(p []byte) (int, error) {
	return b.file().Write(p)
}

func (b *fileBuffer) Close() error {
	return errs.Wrap(b.file().Close())
}

func NewFileBuffer() (streambuf.Buffer, error) {
	fh, err := os.CreateTemp("", "streambuf")
	if err != nil {
		return nil, errs.Wrap(err)
	}
	if err := os.Remove(fh.Name()); err != nil {
		return nil, errs.Wrap(err)
	}
	return (*fileBuffer)(fh), err
}

//
// managed memory buffer pool
//

type ManagedMemoryBufferPool struct {
	InitialCapacity int

	mu   sync.Mutex
	bufs []*streambuf.MemoryBuffer
}

func (mp *ManagedMemoryBufferPool) Clear() {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.bufs = mp.bufs[:cap(mp.bufs)]
	for i := range mp.bufs {
		mp.bufs[i] = nil
	}
	mp.bufs = nil
}

func (mp *ManagedMemoryBufferPool) NewBuffer() streambuf.Buffer {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	var m *streambuf.MemoryBuffer
	if len(mp.bufs) == 0 {
		m = streambuf.NewMemoryBuffer(mp.InitialCapacity)
	} else {
		i := len(mp.bufs) - 1
		m = mp.bufs[i]
		mp.bufs = mp.bufs[:i]
	}

	return &managedMemoryBuffer{
		pool: mp,
		buf:  m,
	}
}

func (mp *ManagedMemoryBufferPool) put(m *streambuf.MemoryBuffer) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	m.Reset()
	mp.bufs = append(mp.bufs, m)
}

type managedMemoryBuffer struct {
	mu   sync.Mutex
	pool *ManagedMemoryBufferPool
	buf  *streambuf.MemoryBuffer
}

func (m *managedMemoryBuffer) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf == nil {
		return 0, errs.New("closed buffer")
	}

	return m.buf.ReadAt(p, off)
}

func (m *managedMemoryBuffer) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf == nil {
		return 0, errs.New("closed buffer")
	}

	return m.buf.Write(p)
}

func (m *managedMemoryBuffer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf != nil {
		m.pool.put(m.buf)
		m.buf = nil
	}

	return nil // Close is a noop for a streambuf.MemoryBuffer
}
