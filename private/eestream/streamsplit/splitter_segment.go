package streamsplit

import (
	"io"

	"storj.io/common/encryption"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

type splitterSegment struct {
	position   metaclient.SegmentPosition
	encryption metaclient.SegmentEncryption
	encParams  storj.EncryptionParameters
	contentKey *storj.Key

	maxSegmentSize int64
	encTransformer encryption.Transformer
	encStreamBuf   *encryptedStreamBuffer
}

func (s *splitterSegment) Begin() metaclient.BatchItem {
	return &metaclient.BeginSegmentParams{
		StreamID:      nil, // set by the stream batcher
		Position:      s.position,
		MaxOrderLimit: s.maxSegmentSize,
	}
}

func (s *splitterSegment) Position() metaclient.SegmentPosition { return s.position }
func (s *splitterSegment) Inline() bool                         { return false }
func (s *splitterSegment) Reader() io.Reader                    { return s.encStreamBuf.Reader() }
func (s *splitterSegment) DoneReading(err error)                { s.encStreamBuf.DoneReading(err) }

func (s *splitterSegment) EncryptETag(eTag []byte) ([]byte, error) {
	return encryptETag(eTag, s.encParams.CipherSuite, s.contentKey)
}

func (s *splitterSegment) Finalize() *SegmentInfo {
	plainSize := s.encStreamBuf.PlainSize()
	return &SegmentInfo{
		Encryption:    s.encryption,
		PlainSize:     plainSize,
		EncryptedSize: encryption.CalcTransformerEncryptedSize(plainSize, s.encTransformer),
	}
}
