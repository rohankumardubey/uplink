package streamsplit

import (
	"context"
	"crypto/rand"
	"io"

	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream/streambuf"
	"storj.io/uplink/private/metaclient"
)

type Segment interface {
	Begin() metaclient.BatchItem
	Position() metaclient.SegmentPosition
	Inline() bool
	Reader() io.Reader
	EncryptETag(eTag []byte) ([]byte, error)
	Finalize() *SegmentInfo
	DoneReading(err error)
}

type SegmentInfo struct {
	Encryption    metaclient.SegmentEncryption
	PlainSize     int64
	EncryptedSize int64
}

type SplitterOptions struct {
	Split      int64
	Minimum    int64
	Params     storj.EncryptionParameters
	Key        *storj.Key
	PartNumber int32
}

type Splitter struct {
	NewBuffer func() (streambuf.Buffer, error)

	split          *baseSplitter
	opts           SplitterOptions
	maxSegmentSize int64
	index          int32
}

func NewSplitter(opts SplitterOptions) (*Splitter, error) {
	maxSegmentSize, err := encryption.CalcEncryptedSize(opts.Split, opts.Params)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return &Splitter{
		NewBuffer: func() (streambuf.Buffer, error) { return NewPooledMemoryBuffer(), nil },

		split:          newBaseSplitter(opts.Split, opts.Minimum),
		opts:           opts,
		maxSegmentSize: maxSegmentSize,
	}, nil
}

func (e *Splitter) Finish(err error)            { e.split.Finish(err) }
func (e *Splitter) Write(p []byte) (int, error) { return e.split.Write(p) }

func (e *Splitter) Next(ctx context.Context) (Segment, error) {
	position := metaclient.SegmentPosition{
		PartNumber: e.opts.PartNumber,
		Index:      e.index,
	}
	var contentKey storj.Key
	var keyNonce storj.Nonce

	// do all of the fallible actions before checking with the splitter
	nonce, err := nonceForPosition(position)
	if err != nil {
		return nil, err
	}
	if _, err := rand.Read(contentKey[:]); err != nil {
		return nil, errs.Wrap(err)
	}
	if _, err := rand.Read(keyNonce[:]); err != nil {
		return nil, errs.Wrap(err)
	}
	enc, err := encryption.NewEncrypter(e.opts.Params.CipherSuite, &contentKey, &nonce, int(e.opts.Params.BlockSize))
	if err != nil {
		return nil, errs.Wrap(err)
	}
	encKey, err := encryption.EncryptKey(&contentKey, e.opts.Params.CipherSuite, e.opts.Key, &keyNonce)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	buffer, err := e.NewBuffer()
	if err != nil {
		return nil, errs.Wrap(err)
	}

	sbuf := streambuf.NewStreamBuffer(buffer, e.opts.Minimum)
	wrc := encryption.TransformWriterPadded(sbuf, enc)
	esbuf := newEncryptedStreamBuffer(sbuf, wrc)
	segEncryption := metaclient.SegmentEncryption{
		EncryptedKeyNonce: keyNonce,
		EncryptedKey:      encKey,
	}

	// check for the next segment/inline boundary. if an error, don't update any
	// local state.
	inline, eof, err := e.split.Next(ctx, esbuf)
	switch {
	case err != nil:
		return nil, errs.Wrap(err)

	case eof:
		return nil, nil

	case inline != nil:
		// encrypt the inline data, and update the internal state if it succeeds.
		encData, err := encryption.Encrypt(inline, e.opts.Params.CipherSuite, &contentKey, &nonce)
		if err != nil {
			return nil, errs.Wrap(err)
		}

		// everything fallible is done. update the internal state.
		e.index++

		return &splitterInline{
			position:   position,
			encryption: segEncryption,
			encParams:  e.opts.Params,
			contentKey: &contentKey,

			encData:   encData,
			plainSize: int64(len(inline)),
		}, nil

	default:
		// everything fallible is done. update the internal state.
		e.index++

		return &splitterSegment{
			position:   position,
			encryption: segEncryption,
			encParams:  e.opts.Params,
			contentKey: &contentKey,

			maxSegmentSize: e.maxSegmentSize,
			encTransformer: enc,
			encStreamBuf:   esbuf,
		}, nil
	}
}
