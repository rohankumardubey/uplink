// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package metainfo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"storj.io/common/memory"
	"storj.io/common/testcontext"
	"storj.io/common/testrand"
	"storj.io/storj/private/testplanet"
	"storj.io/storj/satellite/metabase"
	"storj.io/uplink/private/testuplink"
)

func TestMultisegmentUploadWithLastInline(t *testing.T) {
	// this is special case were uploaded object has 3 segments (2 remote + 1 inline)
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		satellite := planet.Satellites[0]
		uplink := planet.Uplinks[0]

		expectedData := testrand.Bytes(40*memory.KiB + 1)

		err := planet.Uplinks[0].Upload(ctx, planet.Satellites[0], "testbucket", "test/path", expectedData)
		require.NoError(t, err)

		downloaded, err := uplink.Download(ctx, satellite, "testbucket", "test/path")
		require.NoError(t, err)
		require.Equal(t, expectedData, downloaded)

		// verify that object has 3 segments, 2 remote + 1 inline
		objects, err := planet.Satellites[0].Metabase.DB.TestingAllCommittedObjects(ctx, planet.Uplinks[0].Projects[0].ID, "testbucket")
		require.NoError(t, err)
		require.Len(t, objects, 1)

		segments, err := planet.Satellites[0].Metabase.DB.TestingAllObjectSegments(ctx, metabase.ObjectLocation{
			ProjectID:  planet.Uplinks[0].Projects[0].ID,
			BucketName: "testbucket",
			ObjectKey:  objects[0].ObjectKey,
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(segments))

		assert.EqualValues(t, segments[0].PlainSize, 20*memory.KiB)
		assert.Empty(t, segments[0].InlineData)

		assert.EqualValues(t, segments[1].PlainSize, 20*memory.KiB)
		assert.Empty(t, segments[1].InlineData)

		assert.EqualValues(t, segments[2].PlainSize, 1)
		assert.NotEmpty(t, segments[2].InlineData)
	})
}

func TestDownloadMigratedObject(t *testing.T) {
	testplanet.Run(t, testplanet.Config{
		SatelliteCount: 1, StorageNodeCount: 4, UplinkCount: 1,
		Reconfigure: testplanet.Reconfigure{
			Satellite: testplanet.MaxSegmentSize(20 * memory.KiB),
		},
	}, func(t *testing.T, ctx *testcontext.Context, planet *testplanet.Planet) {
		// this configures uplink to NOT send plain size while upload
		// such uploaded object will look like object after migration from pointerdb
		newCtx := testuplink.WithoutPlainSize(ctx)

		{ // inline segment
			expectedData := testrand.Bytes(1 * memory.KiB)
			err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "bucket", "inline-segment", expectedData)
			require.NoError(t, err)

			data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], "bucket", "inline-segment")
			require.NoError(t, err)
			require.Equal(t, expectedData, data)
		}

		{ // single segment
			expectedData := testrand.Bytes(10 * memory.KiB)
			err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "bucket", "single-segment", expectedData)
			require.NoError(t, err)

			data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], "bucket", "single-segment")
			require.NoError(t, err)
			require.Equal(t, expectedData, data)
		}

		{ // many segments
			expectedData := testrand.Bytes(100 * memory.KiB)
			err := planet.Uplinks[0].Upload(newCtx, planet.Satellites[0], "bucket", "many-segments", expectedData)
			require.NoError(t, err)

			data, err := planet.Uplinks[0].Download(newCtx, planet.Satellites[0], "bucket", "many-segments")
			require.NoError(t, err)
			require.Equal(t, expectedData, data)
		}
	})
}
