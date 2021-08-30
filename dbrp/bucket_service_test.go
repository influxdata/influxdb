package dbrp

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp/mocks"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var generator = snowflake.NewDefaultIDGenerator()

func TestBucketService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ctx       = context.Background()
		bucketID  = generator.ID()
		orgID     = generator.ID()
		mappingID = generator.ID()

		logger            = zap.NewNop()
		bucketServiceMock = mocks.NewMockBucketService(ctrl)
		dbrpService       = mocks.NewMockDBRPMappingService(ctrl)

		bucket = &influxdb.Bucket{
			ID:    bucketID,
			OrgID: orgID,
		}
	)

	findBucket := bucketServiceMock.EXPECT().
		FindBucketByID(gomock.Any(), bucketID).
		Return(bucket, nil)
	deleteBucket := bucketServiceMock.EXPECT().
		DeleteBucket(gomock.Any(), bucketID).
		Return(nil)

	findMapping := dbrpService.EXPECT().
		FindMany(gomock.Any(), influxdb.DBRPMappingFilter{
			BucketID: &bucketID,
			OrgID:    &orgID,
		}).Return([]*influxdb.DBRPMapping{
		{ID: mappingID},
	}, 1, nil)
	deleteMapping := dbrpService.EXPECT().
		Delete(gomock.Any(), orgID, mappingID).
		Return(nil)

	gomock.InOrder(
		findBucket,
		deleteBucket,
		findMapping,
		deleteMapping,
	)

	bucketService := NewBucketService(logger, bucketServiceMock, dbrpService)
	err := bucketService.DeleteBucket(ctx, bucketID)
	require.NoError(t, err)
}
