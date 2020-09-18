package streams

import (
	"sync"

	"github.com/zeebo/errs"
	"storj.io/uplink/private/eestream/streamsplit"
)

type uploadResult struct {
	info UploadInfo
	err  error
}

type Upload struct {
	mu     sync.Mutex
	split  *streamsplit.Splitter
	done   chan uploadResult
	info   UploadInfo
	cancel func()
}

func (u *Upload) Write(p []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	return u.split.Write(p)
}

func (u *Upload) Abort() error {
	u.split.Finish(errs.New("aborted"))
	u.cancel()

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.done == nil {
		return errs.New("upload already done")
	}
	<-u.done
	u.done = nil

	return nil
}

func (u *Upload) Commit() error {
	u.split.Finish(nil)

	u.mu.Lock()
	defer u.mu.Unlock()

	if u.done == nil {
		return errs.New("upload already done")
	}
	result := <-u.done
	u.info = result.info
	u.done = nil

	u.cancel()
	return result.err
}

func (u *Upload) Meta() *Meta {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.done != nil {
		return nil
	}

	return &Meta{
		Modified: u.info.CreationDate,
		Size:     u.info.PlainSize,
	}
}
