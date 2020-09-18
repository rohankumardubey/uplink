// Copyright (C) 2023 Storj Labs, Inc.
// See LICENSE for copying information.

package segmentupload

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/uplink/private/eestream"
	"storj.io/uplink/private/eestream/scheduler"
	"storj.io/uplink/private/eestream/streamsplit"
	"storj.io/uplink/private/metaclient"
	"storj.io/uplink/private/storage/streams/pieceupload"
)

var (
	// safetyMargin is maximum number of uploads above the optimal threshold
	// that will be attempted. It is overriden by tests.
	safetyMargin = 15
)

type Scheduler interface {
	Join() scheduler.Handle
}

func Begin(ctx context.Context,
	beginSegment *metaclient.BeginSegmentResponse,
	segment streamsplit.Segment,
	limitsExchanger pieceupload.LimitsExchanger,
	piecePutter pieceupload.PiecePutter,
	scheduler Scheduler,
) (_ *Upload, err error) {
	// Join the scheduler so the concurrency can be limited appropriately.
	handle := scheduler.Join()
	defer func() {
		if err != nil {
			handle.Done()
		}
	}()

	if beginSegment.RedundancyStrategy.ErasureScheme == nil {
		return nil, errs.New("begin segment response is missing redundancy strategy")
	}
	if beginSegment.PiecePrivateKey.IsZero() {
		return nil, errs.New("begin segment response is missing piece private key")
	}

	optimalThreshold := beginSegment.RedundancyStrategy.OptimalThreshold()
	if optimalThreshold > len(beginSegment.Limits) {
		return nil, errs.New("begin segment response needs at least %d limits to meet optimal threshold but has %d", optimalThreshold, len(beginSegment.Limits))
	}

	// The number of uploads is enough to satisfy the optimal threshold plus
	// a small safety margin, capped by the number of limits.
	uploaderCount := optimalThreshold + safetyMargin
	if uploaderCount > len(beginSegment.Limits) {
		uploaderCount = len(beginSegment.Limits)
	}

	mgr := pieceupload.NewManager(
		limitsExchanger,
		&pieceReader{segment, beginSegment.RedundancyStrategy},
		beginSegment.SegmentID,
		beginSegment.Limits,
	)

	// Create a context that we can use to cancel piece uploads when we have enough.
	longTailCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	results := make(chan segmentResult, uploaderCount)
	var successful int32
	for i := 0; i < uploaderCount; i++ {
		res, ok := handle.Get(ctx)
		if !ok {
			return nil, errs.New("failed to obtain piece upload resource")
		}

		go func() {
			// Whether the upload is ultimately successful or not, when this
			// function returns, the scheduler resource MUST be released to
			// allow other piece uploads to take place.
			defer res.Done()
			uploaded, err := pieceupload.UploadOne(longTailCtx, ctx, mgr, piecePutter, beginSegment.PiecePrivateKey)
			results <- segmentResult{uploaded: uploaded, err: err}
			if uploaded {
				// Piece upload was successful. If we have met or surpassed the
				// optimal threshold, we can cancel the rest.
				if int(atomic.AddInt32(&successful, 1)) >= optimalThreshold {
					cancel()
				}
			}
		}()
	}

	return &Upload{
		ctx:              ctx,
		optimalThreshold: beginSegment.RedundancyStrategy.OptimalThreshold(),
		handle:           handle,
		results:          results,
		cancel:           cancel,
		mgr:              mgr,
		segment:          segment,
	}, nil
}

type pieceReader struct {
	segment    streamsplit.Segment
	redundancy eestream.RedundancyStrategy
}

func (r *pieceReader) PieceReader(num int) io.Reader {
	segment := r.segment.Reader()
	stripeSize := r.redundancy.StripeSize()
	paddedData := encryption.PadReader(io.NopCloser(segment), stripeSize)
	return NewEncodedReader(paddedData, r.redundancy, num)
}

type segmentResult struct {
	uploaded bool
	err      error
}

type Upload struct {
	ctx              context.Context
	optimalThreshold int
	handle           scheduler.Handle
	results          chan segmentResult
	cancel           context.CancelFunc
	mgr              *pieceupload.Manager
	segment          streamsplit.Segment
}

func (upload *Upload) Wait() (*metaclient.CommitSegmentParams, error) {
	defer upload.handle.Done()
	defer upload.cancel()

	var eg errs.Group
	var successful int
	for i := 0; i < cap(upload.results); i++ {
		result := <-upload.results
		if result.uploaded {
			successful++
		}
		eg.Add(result.err)
	}

	var err error
	if successful < upload.optimalThreshold {
		err = errs.Combine(errs.New("failed to upload enough pieces (needed at least %d but got %d)", upload.optimalThreshold, successful), eg.Err())
	}
	upload.segment.DoneReading(err)
	if err != nil {
		return nil, err
	}

	info := upload.segment.Finalize()
	segmentID, results := upload.mgr.Results()

	return &metaclient.CommitSegmentParams{
		SegmentID:         segmentID,
		Encryption:        info.Encryption,
		SizeEncryptedData: info.EncryptedSize,
		PlainSize:         info.PlainSize,
		EncryptedTag:      nil, // eTag handling is done at a differnt layer
		UploadResult:      results,
	}, nil
}
