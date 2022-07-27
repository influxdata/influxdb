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

func NewMatcher(customMatcher func(arg interface{}) bool) gomock.Matcher {
	return matcherCustomizer{customMatcher}
}

type matcherCustomizer struct {
	matcherFunction func(arg interface{}) bool
}

func (o matcherCustomizer) Matches(x interface{}) bool {
	return o.matcherFunction(x)
}

func (o matcherCustomizer) String() string {
	return "[call back function matcher has returned false]"
}

var generator = snowflake.NewDefaultIDGenerator()

func TestBucketServiceDelete(t *testing.T) {
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

func TestBucketServiceCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ctx             = context.Background()
		bucketID        = generator.ID()
		orgID           = generator.ID()
		bucketName      = "b0"
		retentionPolicy = "autogen"

		logger            = zap.NewNop()
		bucketServiceMock = mocks.NewMockBucketService(ctrl)
		dbrpService       = mocks.NewMockDBRPMappingService(ctrl)

		bucket = &influxdb.Bucket{
			ID:    bucketID,
			Name:  bucketName,
			OrgID: orgID,
		}
	)

	// findBucket := bucketServiceMock.EXPECT().
	// 	FindBucketByID(gomock.Any(), bucketID).
	// 	Return(bucket, nil)

	createBucket := bucketServiceMock.EXPECT().
		CreateBucket(gomock.Any(), bucket).
		Return(nil)

	// findMapping := dbrpService.EXPECT().
	// 	FindMany(gomock.Any(), influxdb.DBRPMappingFilter{
	// 		BucketID: &bucketID,
	// 		OrgID:    &orgID,
	// 	}).Return([]*influxdb.DBRPMapping{
	// 	{ID: mappingID},
	// }, 1, nil)

	mapping := &influxdb.DBRPMapping{
		Database:        bucketName,
		RetentionPolicy: retentionPolicy,
		OrganizationID:  orgID,
		BucketID:        bucketID,
	}
	createMapping := dbrpService.EXPECT().
		Create(gomock.Any(), NewMatcher(mapping.MatchesIgnoreID)).
		Return(nil)

	gomock.InOrder(
		createBucket,
		createMapping,
	)

	bucketService := NewBucketService(logger, bucketServiceMock, dbrpService)
	err := bucketService.CreateBucket(ctx, bucket)
	require.NoError(t, err)
}

func TestBucketServiceUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ctx               = context.Background()
		bucketID          = generator.ID()
		orgID             = generator.ID()
		mappingID         = generator.ID()
		bucketName        = "b0"
		updatedBucketName = "b1"
		retentionPolicy   = "autogen"

		logger            = zap.NewNop()
		bucketServiceMock = mocks.NewMockBucketService(ctrl)
		dbrpService       = mocks.NewMockDBRPMappingService(ctrl)

		bucket = &influxdb.Bucket{
			ID:    bucketID,
			Name:  bucketName,
			OrgID: orgID,
		}
		patchedBucket = &influxdb.Bucket{
			ID:    bucketID,
			Name:  updatedBucketName,
			OrgID: orgID,
		}
	)

	findBucket := bucketServiceMock.EXPECT().
		FindBucketByID(gomock.Any(), bucketID).
		Return(bucket, nil)

	createBucket := bucketServiceMock.EXPECT().
		CreateBucket(gomock.Any(), bucket).
		Return(nil)

	findMapping := dbrpService.EXPECT().
		FindMany(gomock.Any(), influxdb.DBRPMappingFilter{
			BucketID:        &bucketID,
			OrgID:           &orgID,
			Database:        &bucketName,
			RetentionPolicy: &retentionPolicy,
		}).Return([]*influxdb.DBRPMapping{
		{
			ID:             mappingID,
			OrganizationID: orgID,
			BucketID:       bucketID,
		},
	}, 1, nil)

	mapping := &influxdb.DBRPMapping{
		Database:        bucketName,
		RetentionPolicy: retentionPolicy,
		OrganizationID:  orgID,
		BucketID:        bucketID,
	}

	upd := influxdb.BucketUpdate{
		Name: &updatedBucketName,
	}
	createMapping := dbrpService.EXPECT().
		Create(gomock.Any(), NewMatcher(mapping.MatchesIgnoreID)).
		Return(nil)

	patchedMapping := &influxdb.DBRPMapping{
		Database:        updatedBucketName,
		RetentionPolicy: retentionPolicy,
		OrganizationID:  orgID,
		BucketID:        bucketID,
	}

	updateMapping := dbrpService.EXPECT().
		Update(gomock.Any(), NewMatcher(patchedMapping.MatchesIgnoreID)).
		Return(nil)

	updateBucket := bucketServiceMock.EXPECT().
		UpdateBucket(gomock.Any(), bucketID, upd).
		Return(patchedBucket, nil)

	gomock.InOrder(
		createBucket,
		createMapping,
		findBucket,
		updateBucket,
		findMapping,
		updateMapping,
	)

	bucketService := NewBucketService(logger, bucketServiceMock, dbrpService)
	err := bucketService.CreateBucket(ctx, bucket)
	require.NoError(t, err)

	_, err = bucketService.UpdateBucket(ctx, bucketID, upd)
	require.NoError(t, err)
}
