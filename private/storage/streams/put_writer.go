// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/zeebo/errs"
	"storj.io/common/encryption"
	"storj.io/common/paths"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/eestream/streamsplit"
	"storj.io/uplink/private/metaclient"
)

func (s *Store) PutWriter(ctx context.Context, bucket, unencryptedKey string, metadata Metadata, expiration time.Time, sched *scheduler.Scheduler) (_ *Upload, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	done := make(chan uploadResult, 1)

	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	encPath, err := encryption.EncryptPathWithStoreCipher(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	split, err := streamsplit.NewSplitter(streamsplit.SplitterOptions{
		Split:      s.segmentSize,
		Minimum:    int64(s.inlineThreshold),
		Params:     s.encryptionParameters,
		Key:        derivedKey,
		PartNumber: 0,
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		split.Finish(ctx.Err())
	}()

	beginObject := &metaclient.BeginObjectParams{
		Bucket:               []byte(bucket),
		EncryptedObjectKey:   []byte(encPath.Raw()),
		ExpiresAt:            expiration,
		EncryptionParameters: s.encryptionParameters,
	}

	encMeta := s.newEncryptedMetadata(metadata, derivedKey)

	go func() {
		info, err := UploadObject(
			ctx,
			beginObject,
			encMeta,
			split,
			s.metainfo,
			s.ec,
			sched,
		)
		// On failure, we need to "finish" the splitter with an error so that
		// outstanding writes to the splitter fail, otherwise the writes will
		// block waiting for the upload to read the stream.
		if err != nil {
			split.Finish(errs.Combine(errs.New("upload failed"), err))
		}
		done <- uploadResult{info: info, err: err}
	}()

	return &Upload{
		split:  split,
		done:   done,
		cancel: cancel,
	}, nil
}

func (s *Store) newEncryptedMetadata(metadata Metadata, derivedKey *storj.Key) EncryptedMetadata {
	return &encryptedMetadata{
		metadata:    metadata,
		segmentSize: s.segmentSize,
		derivedKey:  derivedKey,
		cipherSuite: s.encryptionParameters.CipherSuite,
	}
}

type encryptedMetadata struct {
	metadata    Metadata
	segmentSize int64
	derivedKey  *storj.Key
	cipherSuite storj.CipherSuite
}

func (e *encryptedMetadata) EncryptedMetadata(lastSegmentSize int64) (data []byte, encKey *storj.EncryptedPrivateKey, nonce *storj.Nonce, err error) {
	metadataBytes, err := e.metadata.Metadata()
	if err != nil {
		return nil, nil, nil, err
	}

	streamInfo, err := pb.Marshal(&pb.StreamInfo{
		SegmentsSize:    e.segmentSize,
		LastSegmentSize: lastSegmentSize,
		Metadata:        metadataBytes,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	var metadataKey storj.Key
	if _, err := rand.Read(metadataKey[:]); err != nil {
		return nil, nil, nil, err
	}

	var encryptedMetadataKeyNonce storj.Nonce
	if _, err := rand.Read(encryptedMetadataKeyNonce[:]); err != nil {
		return nil, nil, nil, err
	}

	// encrypt the metadata key with the derived key and the random encrypted key nonce
	encryptedMetadataKey, err := encryption.EncryptKey(&metadataKey, e.cipherSuite, e.derivedKey, &encryptedMetadataKeyNonce)
	if err != nil {
		return nil, nil, nil, err
	}

	// encrypt the stream info with the metadata key and the zero nonce
	encryptedStreamInfo, err := encryption.Encrypt(streamInfo, e.cipherSuite, &metadataKey, &storj.Nonce{})
	if err != nil {
		return nil, nil, nil, err
	}

	streamMeta, err := pb.Marshal(&pb.StreamMeta{
		EncryptedStreamInfo: encryptedStreamInfo,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	return streamMeta, &encryptedMetadataKey, &encryptedMetadataKeyNonce, nil
}

func (s *Store) PutWriterPart(ctx context.Context, bucket, unencryptedKey string, streamID storj.StreamID, partNumber int32, eTag <-chan []byte, sched *scheduler.Scheduler) (_ *Upload, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	done := make(chan uploadResult, 1)

	derivedKey, err := encryption.DeriveContentKey(bucket, paths.NewUnencrypted(unencryptedKey), s.encStore)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	split, err := streamsplit.NewSplitter(streamsplit.SplitterOptions{
		Split:      s.segmentSize,
		Minimum:    int64(s.inlineThreshold),
		Params:     s.encryptionParameters,
		Key:        derivedKey,
		PartNumber: partNumber,
	})
	if err != nil {
		return nil, errs.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		split.Finish(ctx.Err())
	}()

	go func() {
		info, err := UploadPart(
			ctx,
			streamID,
			split,
			s.metainfo,
			s.ec,
			sched,
			eTag,
		)
		// On failure, we need to "finish" the splitter with an error so that
		// outstanding writes to the splitter fail, otherwise the writes will
		// block waiting for the upload to read the stream.
		if err != nil {
			split.Finish(errs.Combine(errs.New("upload failed"), err))
		}
		done <- uploadResult{info: info, err: err}
	}()

	return &Upload{
		split:  split,
		done:   done,
		cancel: cancel,
	}, nil
}
