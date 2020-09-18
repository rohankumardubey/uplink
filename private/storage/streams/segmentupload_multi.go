// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package streams

import (
	"context"

	"github.com/zeebo/errs"
	"golang.org/x/sync/errgroup"

	"storj.io/common/context2"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/eestream/streamsplit"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/batchaggregator"
	"storj.io/uplink/private/storage/streams/pieceupload"
	"storj.io/uplink/private/storage/streams/segmenttracker"
	"storj.io/uplink/private/storage/streams/segmentupload"
	"storj.io/uplink/private/storage/streams/streambatcher"
)

type Source interface {
	// Next returns the next segment. It will return all-nil when there are no
	// more segments left.
	Next(context.Context) (streamsplit.Segment, error)
}

type EncryptedMetadata interface {
	EncryptedMetadata(lastSegmentSize int64) (data []byte, encKey *storj.EncryptedPrivateKey, nonce *storj.Nonce, err error)
}

type MetainfoClient interface {
	metaclient.Batcher
	RetryBeginSegmentPieces(ctx context.Context, params metaclient.RetryBeginSegmentPiecesParams) (metaclient.RetryBeginSegmentPiecesResponse, error)
}

type UploadInfo = streambatcher.Info

func UploadObject(ctx context.Context, beginObject *metaclient.BeginObjectParams, encMeta EncryptedMetadata, segmentSource Source, miClient MetainfoClient, piecePutter pieceupload.PiecePutter, sched *scheduler.Scheduler) (UploadInfo, error) {
	return uploadSegments(ctx, segmentSource, miClient, piecePutter, sched, beginObject, encMeta, nil, nil)
}

func UploadPart(ctx context.Context, streamID storj.StreamID, segmentSource Source, miClient MetainfoClient, piecePutter pieceupload.PiecePutter, sched *scheduler.Scheduler, eTagCh <-chan []byte) (UploadInfo, error) {
	return uploadSegments(ctx, segmentSource, miClient, piecePutter, sched, nil, nil, streamID, eTagCh)
}

func uploadSegments(ctx context.Context, segmentSource Source, miClient MetainfoClient, piecePutter pieceupload.PiecePutter, sched *scheduler.Scheduler, beginObject *metaclient.BeginObjectParams, encMeta EncryptedMetadata, streamID storj.StreamID, eTagCh <-chan []byte) (_ UploadInfo, err error) {
	batcher := streambatcher.New(miClient, streamID)
	aggregator := batchaggregator.New(batcher)

	if beginObject != nil {
		aggregator.Schedule(beginObject)
		defer func() {
			if err != nil {
				if batcherStreamID := batcher.StreamID(); !batcherStreamID.IsZero() {
					if deleteErr := deleteCancelledObject(ctx, miClient, beginObject, batcherStreamID); deleteErr != nil {
						mon.Event("failed to delete cancelled object")
					}
				}
			}
		}()
	}

	tracker := segmenttracker.New(aggregator, eTagCh)

	var segments []streamsplit.Segment
	defer func() {
		for _, segment := range segments {
			segment.DoneReading(err)
		}
	}()

	var eg errgroup.Group
	defer func() { _ = eg.Wait() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		segment, err := segmentSource.Next(ctx)
		if err != nil {
			return UploadInfo{}, err
		} else if segment == nil {
			break
		}
		segments = append(segments, segment)

		if segment.Inline() {
			tracker.SegmentDone(segment, segment.Begin())
			break
		}

		// Start the upload. Use a closure to ensure the segment is closed on
		// failure.
		resp, err := aggregator.ScheduleAndFlush(ctx, segment.Begin())
		if err != nil {
			return UploadInfo{}, err
		}

		beginSegment, err := resp.BeginSegment()
		if err != nil {
			return UploadInfo{}, err
		}

		upload, err := segmentupload.Begin(ctx, &beginSegment, segment, limitsExchanger{miClient}, piecePutter, sched)
		if err != nil {
			return UploadInfo{}, err
		}

		eg.Go(func() error {
			commitSegment, err := upload.Wait()
			if err != nil {
				// an upload has failed so we should cancel the rest of the uploads
				cancel()
				return err
			}
			tracker.SegmentDone(segment, commitSegment)
			return nil
		})
	}

	if len(segments) == 0 {
		return UploadInfo{}, errs.New("programmer error: there should always be at least one segment")
	}

	lastSegment := segments[len(segments)-1]

	tracker.SegmentsScheduled(lastSegment)

	if err := eg.Wait(); err != nil {
		return UploadInfo{}, err
	}

	if err := tracker.Flush(ctx); err != nil {
		return UploadInfo{}, err
	}

	// we need to schedule a commit object if we had a begin object
	if beginObject != nil {
		commitObject, err := createCommitObjectParams(lastSegment, encMeta)
		if err != nil {
			return UploadInfo{}, err
		}
		aggregator.Schedule(commitObject)
	}

	if err := aggregator.Flush(ctx); err != nil {
		return UploadInfo{}, err
	}

	return batcher.Info()
}

func createCommitObjectParams(lastSegment streamsplit.Segment, encMeta EncryptedMetadata) (*metaclient.CommitObjectParams, error) {
	lastSegmentSize := int64(0)
	if lastSegment != nil {
		info := lastSegment.Finalize()
		lastSegmentSize = info.PlainSize
	}

	encryptedMetadata, encryptedMetadataKey, encryptedMetadataKeyNonce, err := encMeta.EncryptedMetadata(lastSegmentSize)
	if err != nil {
		return nil, err
	}

	return &metaclient.CommitObjectParams{
		StreamID:                      nil, // set by the stream batcher
		EncryptedMetadataNonce:        *encryptedMetadataKeyNonce,
		EncryptedMetadataEncryptedKey: *encryptedMetadataKey,
		EncryptedMetadata:             encryptedMetadata,
	}, nil
}

func deleteCancelledObject(ctx context.Context, miClient MetainfoClient, beginObject *metaclient.BeginObjectParams, streamID storj.StreamID) (err error) {
	defer mon.Task()(&ctx)(&err)

	ctx = context2.WithoutCancellation(ctx)
	_, err = miClient.Batch(ctx, &metaclient.BeginDeleteObjectParams{
		Bucket:             beginObject.Bucket,
		EncryptedObjectKey: beginObject.EncryptedObjectKey,
		// TODO remove it or set to 0 when satellite side will be fixed
		Version:  1,
		StreamID: streamID,
		Status:   int32(pb.Object_UPLOADING),
	})
	return err
}

type limitsExchanger struct {
	miClient MetainfoClient
}

func (e limitsExchanger) ExchangeLimits(ctx context.Context, segmentID storj.SegmentID, pieceNumbers []int) (storj.SegmentID, []*pb.AddressedOrderLimit, error) {
	resp, err := e.miClient.RetryBeginSegmentPieces(ctx, metaclient.RetryBeginSegmentPiecesParams{
		SegmentID:         segmentID,
		RetryPieceNumbers: pieceNumbers,
	})
	if err != nil {
		return nil, nil, err
	}
	return resp.SegmentID, resp.Limits, nil
}
