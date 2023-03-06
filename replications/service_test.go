package replications

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/models"
	replicationsMock "github.com/influxdata/influxdb/v2/replications/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

//go:generate go run github.com/golang/mock/mockgen -package mock -destination ./mock/validator.go github.com/influxdata/influxdb/v2/replications ReplicationValidator
//go:generate go run github.com/golang/mock/mockgen -package mock -destination ./mock/bucket_service.go github.com/influxdata/influxdb/v2/replications BucketService
//go:generate go run github.com/golang/mock/mockgen -package mock -destination ./mock/queue_management.go github.com/influxdata/influxdb/v2/replications DurableQueueManager
//go:generate go run github.com/golang/mock/mockgen -package mock -destination ./mock/points_writer.go github.com/influxdata/influxdb/v2/storage PointsWriter
//go:generate go run github.com/golang/mock/mockgen -package mock -destination ./mock/service_store.go github.com/influxdata/influxdb/v2/replications ServiceStore

var (
	ctx          = context.Background()
	orgID        = platform.ID(10)
	id1          = platform.ID(1)
	id2          = platform.ID(2)
	desc         = "testing testing"
	replication1 = influxdb.Replication{
		ID:                id1,
		OrgID:             orgID,
		Name:              "test",
		Description:       &desc,
		RemoteID:          platform.ID(100),
		LocalBucketID:     platform.ID(1000),
		RemoteBucketID:    idPointer(99999),
		MaxQueueSizeBytes: 3 * influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	replication2 = influxdb.Replication{
		ID:                id2,
		OrgID:             orgID,
		Name:              "test",
		Description:       &desc,
		RemoteID:          platform.ID(100),
		LocalBucketID:     platform.ID(1000),
		RemoteBucketID:    idPointer(99999),
		MaxQueueSizeBytes: 3 * influxdb.DefaultReplicationMaxQueueSizeBytes,
	}
	createReq = influxdb.CreateReplicationRequest{
		OrgID:             replication1.OrgID,
		Name:              replication1.Name,
		Description:       replication1.Description,
		RemoteID:          replication1.RemoteID,
		LocalBucketID:     replication1.LocalBucketID,
		RemoteBucketID:    *replication1.RemoteBucketID,
		MaxQueueSizeBytes: replication1.MaxQueueSizeBytes,
	}
	newRemoteID          = platform.ID(200)
	newQueueSize         = influxdb.MinReplicationMaxQueueSizeBytes
	updateReqWithNewSize = influxdb.UpdateReplicationRequest{
		RemoteID:          &newRemoteID,
		MaxQueueSizeBytes: &newQueueSize,
	}
	updatedReplicationWithNewSize = influxdb.Replication{
		ID:                replication1.ID,
		OrgID:             replication1.OrgID,
		Name:              replication1.Name,
		Description:       replication1.Description,
		RemoteID:          *updateReqWithNewSize.RemoteID,
		LocalBucketID:     replication1.LocalBucketID,
		RemoteBucketID:    replication1.RemoteBucketID,
		MaxQueueSizeBytes: *updateReqWithNewSize.MaxQueueSizeBytes,
	}
	updateReqWithNoNewSize = influxdb.UpdateReplicationRequest{
		RemoteID: &newRemoteID,
	}
	updatedReplicationWithNoNewSize = influxdb.Replication{
		ID:                replication1.ID,
		OrgID:             replication1.OrgID,
		Name:              replication1.Name,
		Description:       replication1.Description,
		RemoteID:          *updateReqWithNewSize.RemoteID,
		LocalBucketID:     replication1.LocalBucketID,
		RemoteBucketID:    replication1.RemoteBucketID,
		MaxQueueSizeBytes: replication1.MaxQueueSizeBytes,
	}
	remoteID   = platform.ID(888888)
	httpConfig = influxdb.ReplicationHTTPConfig{
		RemoteURL:        fmt.Sprintf("http://%s.cloud", replication1.RemoteID),
		RemoteToken:      replication1.RemoteID.String(),
		RemoteOrgID:      &remoteID,
		AllowInsecureTLS: true,
		RemoteBucketID:   replication1.RemoteBucketID,
	}
)

func idPointer(id int) *platform.ID {
	p := platform.ID(id)
	return &p
}

func TestListReplications(t *testing.T) {
	t.Parallel()

	filter := influxdb.ReplicationListFilter{}

	tests := []struct {
		name                          string
		list                          influxdb.Replications
		ids                           []platform.ID
		sizes                         map[platform.ID]int64
		rsizes                        map[platform.ID]int64
		storeErr                      error
		queueManagerErr               error
		queueManagerRemainingSizesErr error
	}{
		{
			name: "matches multiple",
			list: influxdb.Replications{
				Replications: []influxdb.Replication{replication1, replication2},
			},
			ids:   []platform.ID{replication1.ID, replication2.ID},
			sizes: map[platform.ID]int64{replication1.ID: 1000, replication2.ID: 2000},
		},
		{
			name: "matches one",
			list: influxdb.Replications{
				Replications: []influxdb.Replication{replication1},
			},
			ids:   []platform.ID{replication1.ID},
			sizes: map[platform.ID]int64{replication1.ID: 1000},
		},
		{
			name: "matches none",
			list: influxdb.Replications{},
		},
		{
			name:     "store error",
			storeErr: errors.New("error from store"),
		},
		{
			name: "queue manager error",
			list: influxdb.Replications{
				Replications: []influxdb.Replication{replication1},
			},
			ids:             []platform.ID{replication1.ID},
			queueManagerErr: errors.New("error from queue manager"),
		},
		{
			name: "queue manager error - remaining queue size",
			list: influxdb.Replications{
				Replications: []influxdb.Replication{replication1},
			},
			ids:                           []platform.ID{replication1.ID},
			queueManagerRemainingSizesErr: errors.New("Remaining Queue Size erro"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().ListReplications(gomock.Any(), filter).Return(&tt.list, tt.storeErr)

			if tt.storeErr == nil && len(tt.list.Replications) > 0 {
				mocks.durableQueueManager.EXPECT().CurrentQueueSizes(tt.ids).Return(tt.sizes, tt.queueManagerErr)
			}

			if tt.storeErr == nil && tt.queueManagerErr == nil && len(tt.list.Replications) > 0 {
				mocks.durableQueueManager.EXPECT().RemainingQueueSizes(tt.ids).Return(tt.rsizes, tt.queueManagerRemainingSizesErr)
			}
			got, err := svc.ListReplications(ctx, filter)

			var wantErr error
			if tt.storeErr != nil {
				wantErr = tt.storeErr
			} else if tt.queueManagerErr != nil {
				wantErr = tt.queueManagerErr
			} else if tt.queueManagerRemainingSizesErr != nil {
				wantErr = tt.queueManagerRemainingSizesErr
			}

			require.Equal(t, wantErr, err)

			if wantErr != nil {
				require.Nil(t, got)
				return
			}

			for _, r := range got.Replications {
				require.Equal(t, tt.sizes[r.ID], r.CurrentQueueSizeBytes)
				require.Equal(t, tt.rsizes[r.ID], r.RemainingBytesToBeSynced)
			}
		})
	}
}

func TestCreateReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		create          influxdb.CreateReplicationRequest
		storeErr        error
		bucketErr       error
		queueManagerErr error
		want            *influxdb.Replication
		wantErr         error
	}{
		{
			name:   "success",
			create: createReq,
			want:   &replication1,
		},
		{
			name:      "bucket service error",
			create:    createReq,
			bucketErr: errors.New("bucket service error"),
			wantErr:   errLocalBucketNotFound(createReq.LocalBucketID, errors.New("bucket service error")),
		},
		{
			name:            "initialize queue error",
			create:          createReq,
			queueManagerErr: errors.New("queue manager error"),
			wantErr:         errors.New("queue manager error"),
		},
		{
			name:     "store create error",
			create:   createReq,
			storeErr: errors.New("store create error"),
			wantErr:  errors.New("store create error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.bucketSvc.EXPECT().RLock()
			mocks.bucketSvc.EXPECT().RUnlock()
			mocks.serviceStore.EXPECT().Lock()
			mocks.serviceStore.EXPECT().Unlock()

			mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), tt.create.LocalBucketID).Return(nil, tt.bucketErr)

			if tt.bucketErr == nil {
				mocks.durableQueueManager.EXPECT().InitializeQueue(id1, tt.create.MaxQueueSizeBytes, tt.create.OrgID, tt.create.LocalBucketID, tt.create.MaxAgeSeconds).Return(tt.queueManagerErr)
			}

			if tt.queueManagerErr == nil && tt.bucketErr == nil {
				mocks.serviceStore.EXPECT().CreateReplication(gomock.Any(), id1, tt.create).Return(tt.want, tt.storeErr)
			}

			if tt.storeErr != nil {
				mocks.durableQueueManager.EXPECT().DeleteQueue(id1).Return(nil)
			}

			got, err := svc.CreateReplication(ctx, tt.create)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestValidateNewReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		req          influxdb.CreateReplicationRequest
		storeErr     error
		bucketErr    error
		validatorErr error
		wantErr      error
	}{
		{
			name: "valid",
			req:  createReq,
		},
		{
			name:      "bucket service error",
			req:       createReq,
			bucketErr: errors.New("bucket service error"),
			wantErr:   errLocalBucketNotFound(createReq.LocalBucketID, errors.New("bucket service error")),
		},
		{
			name:     "store populate error",
			req:      createReq,
			storeErr: errors.New("store populate error"),
			wantErr:  errors.New("store populate error"),
		},
		{
			name:         "validation error - invalid replication",
			req:          createReq,
			validatorErr: errors.New("validation error"),
			wantErr: &ierrors.Error{
				Code: ierrors.EInvalid,
				Msg:  "replication parameters fail validation",
				Err:  errors.New("validation error"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.bucketSvc.EXPECT().FindBucketByID(gomock.Any(), tt.req.LocalBucketID).Return(nil, tt.bucketErr)

			testConfig := &influxdb.ReplicationHTTPConfig{RemoteBucketID: &tt.req.RemoteBucketID}
			if tt.bucketErr == nil {
				mocks.serviceStore.EXPECT().PopulateRemoteHTTPConfig(gomock.Any(), tt.req.RemoteID, testConfig).Return(tt.storeErr)
			}

			if tt.bucketErr == nil && tt.storeErr == nil {
				mocks.validator.EXPECT().ValidateReplication(gomock.Any(), testConfig).Return(tt.validatorErr)
			}

			err := svc.ValidateNewReplication(ctx, tt.req)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestGetReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                          string
		sizes                         map[platform.ID]int64
		rsizes                        map[platform.ID]int64
		storeErr                      error
		queueManagerErr               error
		queueManagerRemainingSizesErr error
		storeWant                     influxdb.Replication
		want                          influxdb.Replication
	}{
		{
			name:      "success",
			sizes:     map[platform.ID]int64{replication1.ID: 1000},
			storeWant: replication1,
			want:      replication1,
		},
		{
			name:     "store error",
			storeErr: errors.New("store error"),
		},
		{
			name:            "queue manager error",
			storeWant:       replication1,
			queueManagerErr: errors.New("queue manager error"),
		},
		{
			name:                          "queue manager error - remaining queue size",
			storeWant:                     replication1,
			queueManagerRemainingSizesErr: errors.New("queue manager error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().GetReplication(gomock.Any(), id1).Return(&tt.storeWant, tt.storeErr)

			if tt.storeErr == nil {
				mocks.durableQueueManager.EXPECT().CurrentQueueSizes([]platform.ID{id1}).Return(tt.sizes, tt.queueManagerErr)
			}
			if tt.storeErr == nil && tt.queueManagerErr == nil {
				mocks.durableQueueManager.EXPECT().RemainingQueueSizes([]platform.ID{id1}).Return(tt.rsizes, tt.queueManagerRemainingSizesErr)
			}

			got, err := svc.GetReplication(ctx, id1)

			var wantErr error
			if tt.storeErr != nil {
				wantErr = tt.storeErr
			} else if tt.queueManagerErr != nil {
				wantErr = tt.queueManagerErr
			} else if tt.queueManagerRemainingSizesErr != nil {
				wantErr = tt.queueManagerRemainingSizesErr
			}

			require.Equal(t, wantErr, err)

			if wantErr != nil {
				require.Nil(t, got)
				return
			}

			require.Equal(t, tt.sizes[got.ID], got.CurrentQueueSizeBytes)
			require.Equal(t, tt.rsizes[got.ID], got.RemainingBytesToBeSynced)

		})
	}
}

func TestUpdateReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                          string
		request                       influxdb.UpdateReplicationRequest
		sizes                         map[platform.ID]int64
		rsizes                        map[platform.ID]int64
		storeErr                      error
		queueManagerUpdateSizeErr     error
		queueManagerCurrentSizesErr   error
		queueManagerRemainingSizesErr error
		storeUpdate                   *influxdb.Replication
		want                          *influxdb.Replication
		wantErr                       error
	}{
		{
			name:        "success with new max queue size",
			request:     updateReqWithNewSize,
			sizes:       map[platform.ID]int64{replication1.ID: *updateReqWithNewSize.MaxQueueSizeBytes},
			storeUpdate: &updatedReplicationWithNewSize,
			want:        &updatedReplicationWithNewSize,
		},
		{
			name:        "success with no new max queue size",
			request:     updateReqWithNoNewSize,
			sizes:       map[platform.ID]int64{replication1.ID: updatedReplicationWithNoNewSize.MaxQueueSizeBytes},
			storeUpdate: &updatedReplicationWithNoNewSize,
			want:        &updatedReplicationWithNoNewSize,
		},
		{
			name:     "store error",
			request:  updateReqWithNoNewSize,
			storeErr: errors.New("store error"),
			wantErr:  errors.New("store error"),
		},
		{
			name:                      "queue manager error - update max queue size",
			request:                   updateReqWithNewSize,
			queueManagerUpdateSizeErr: errors.New("update max size err"),
			wantErr:                   errors.New("update max size err"),
		},
		{
			name:                        "queue manager error - current queue size",
			request:                     updateReqWithNoNewSize,
			queueManagerCurrentSizesErr: errors.New("current size err"),
			storeUpdate:                 &updatedReplicationWithNoNewSize,
			wantErr:                     errors.New("current size err"),
		},
		{
			name:                          "queue manager error - remaining queue size",
			request:                       updateReqWithNoNewSize,
			queueManagerRemainingSizesErr: errors.New("remaining queue size err"),
			storeUpdate:                   &updatedReplicationWithNoNewSize,
			wantErr:                       errors.New("remaining queue size err"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().Lock()
			mocks.serviceStore.EXPECT().Unlock()

			mocks.serviceStore.EXPECT().UpdateReplication(gomock.Any(), id1, tt.request).Return(tt.storeUpdate, tt.storeErr)

			if tt.storeErr == nil && tt.request.MaxQueueSizeBytes != nil {
				mocks.durableQueueManager.EXPECT().UpdateMaxQueueSize(id1, *tt.request.MaxQueueSizeBytes).Return(tt.queueManagerUpdateSizeErr)
			}

			if tt.storeErr == nil && tt.queueManagerUpdateSizeErr == nil {
				mocks.durableQueueManager.EXPECT().CurrentQueueSizes([]platform.ID{id1}).Return(tt.sizes, tt.queueManagerCurrentSizesErr)
			}

			if tt.storeErr == nil && tt.queueManagerUpdateSizeErr == nil && tt.queueManagerCurrentSizesErr == nil {
				mocks.durableQueueManager.EXPECT().RemainingQueueSizes([]platform.ID{id1}).Return(tt.rsizes, tt.queueManagerRemainingSizesErr)
			}

			got, err := svc.UpdateReplication(ctx, id1, tt.request)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestValidateUpdatedReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		request                influxdb.UpdateReplicationRequest
		baseConfig             *influxdb.ReplicationHTTPConfig
		storeGetConfigErr      error
		storePopulateConfigErr error
		validatorErr           error
		want                   error
	}{
		{
			name:       "success",
			request:    updateReqWithNoNewSize,
			baseConfig: &httpConfig,
		},
		{
			name:              "store get full http config error",
			storeGetConfigErr: errors.New("store get full http config error"),
			want:              errors.New("store get full http config error"),
		},
		{
			name:                   "store get populate remote config error",
			request:                updateReqWithNoNewSize,
			storePopulateConfigErr: errors.New("store populate http config error"),
			want:                   errors.New("store populate http config error"),
		},
		{
			name:         "invalid update",
			request:      updateReqWithNoNewSize,
			validatorErr: errors.New("invalid"),
			want: &ierrors.Error{
				Code: ierrors.EInvalid,
				Msg:  "validation fails after applying update",
				Err:  errors.New("invalid"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().GetFullHTTPConfig(gomock.Any(), id1).Return(tt.baseConfig, tt.storeGetConfigErr)

			if tt.storeGetConfigErr == nil {
				mocks.serviceStore.EXPECT().PopulateRemoteHTTPConfig(gomock.Any(), *tt.request.RemoteID, tt.baseConfig).Return(tt.storePopulateConfigErr)
			}

			if tt.storeGetConfigErr == nil && tt.storePopulateConfigErr == nil {
				mocks.validator.EXPECT().ValidateReplication(gomock.Any(), tt.baseConfig).Return(tt.validatorErr)
			}

			err := svc.ValidateUpdatedReplication(ctx, id1, tt.request)
			require.Equal(t, tt.want, err)
		})
	}
}

func TestDeleteReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		storeErr        error
		queueManagerErr error
	}{
		{
			name: "success",
		},
		{
			name:     "store error",
			storeErr: errors.New("store error"),
		},
		{
			name:            "queue manager error",
			queueManagerErr: errors.New("queue manager error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().Lock()
			mocks.serviceStore.EXPECT().Unlock()

			mocks.serviceStore.EXPECT().DeleteReplication(gomock.Any(), id1).Return(tt.storeErr)

			if tt.storeErr == nil {
				mocks.durableQueueManager.EXPECT().DeleteQueue(id1).Return(tt.queueManagerErr)
			}

			err := svc.DeleteReplication(ctx, id1)

			var wantErr error
			if tt.storeErr != nil {
				wantErr = tt.storeErr
			} else if tt.queueManagerErr != nil {
				wantErr = tt.queueManagerErr
			}

			require.Equal(t, wantErr, err)
		})
	}
}

func TestDeleteBucketReplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		storeErr        error
		storeIDs        []platform.ID
		queueManagerErr error
		wantErr         error
	}{
		{
			name:     "success - single replication IDs match bucket ID",
			storeIDs: []platform.ID{id1},
		},
		{
			name:     "success - multiple replication IDs match bucket ID",
			storeIDs: []platform.ID{id1, id2},
		},
		{
			name:     "zero replication IDs match bucket ID",
			storeIDs: []platform.ID{},
		},
		{
			name:     "store error",
			storeErr: errors.New("store error"),
			wantErr:  errors.New("store error"),
		},
		{
			name:            "queue manager delete queue error",
			storeIDs:        []platform.ID{id1},
			queueManagerErr: errors.New("queue manager error"),
			wantErr:         fmt.Errorf("deleting replications for bucket %q failed, see server logs for details", id1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().Lock()
			mocks.serviceStore.EXPECT().Unlock()

			mocks.serviceStore.EXPECT().DeleteBucketReplications(gomock.Any(), id1).Return(tt.storeIDs, tt.storeErr)

			if tt.storeErr == nil {
				for _, id := range tt.storeIDs {
					mocks.durableQueueManager.EXPECT().DeleteQueue(id).Return(tt.queueManagerErr)
				}
			}

			err := svc.DeleteBucketReplications(ctx, id1)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestValidateReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		storeErr     error
		validatorErr error
		wantErr      error
	}{
		{
			name: "valid",
		},
		{
			name:     "store error",
			storeErr: errors.New("store error"),
			wantErr:  errors.New("store error"),
		},
		{
			name:         "validation error - invalid replication",
			validatorErr: errors.New("validation error"),
			wantErr: &ierrors.Error{
				Code: ierrors.EInvalid,
				Msg:  "replication failed validation",
				Err:  errors.New("validation error"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().GetFullHTTPConfig(gomock.Any(), id1).Return(&httpConfig, tt.storeErr)
			if tt.storeErr == nil {
				mocks.validator.EXPECT().ValidateReplication(gomock.Any(), &httpConfig).Return(tt.validatorErr)
			}

			err := svc.ValidateReplication(ctx, id1)
			require.Equal(t, tt.wantErr, err)
		})
	}
}

func TestWritePoints(t *testing.T) {
	t.Parallel()

	svc, mocks := newTestService(t)

	replications := make([]platform.ID, 2)
	replications[0] = replication1.ID
	replications[1] = replication2.ID

	mocks.durableQueueManager.EXPECT().GetReplications(orgID, id1).Return(replications)

	points, err := models.ParsePointsString(`
cpu,host=0 value=1.1 6000000000
cpu,host=A value=1.2 2000000000
cpu,host=A value=1.3 3000000000
cpu,host=B value=1.3 4000000000
cpu,host=B value=1.3 5000000000
cpu,host=C value=1.3 1000000000
mem,host=C value=1.3 1000000000
disk,host=C value=1.3 1000000000`)
	require.NoError(t, err)

	// Points should successfully write to local TSM.
	mocks.pointWriter.EXPECT().WritePoints(gomock.Any(), orgID, id1, points).Return(nil)

	// Points should successfully be enqueued in the 2 replications associated with the local bucket.
	for _, id := range replications {
		mocks.durableQueueManager.EXPECT().
			EnqueueData(id, gomock.Any(), len(points)).
			DoAndReturn(func(_ platform.ID, data []byte, numPoints int) error {
				require.Equal(t, len(points), numPoints)
				checkCompressedData(t, data, points)
				return nil
			})
	}

	require.NoError(t, svc.WritePoints(ctx, orgID, id1, points))
}

func TestWritePointsBatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setupFn func(*testing.T, *service)
	}{
		{
			name: "batch bytes size",
			setupFn: func(t *testing.T, svc *service) {
				t.Helper()
				// Set batch size to smaller size for testing (should result in 3 batches sized 93, 93, and 63 - total size 249)
				svc.maxRemoteWriteBatchSize = 100
			},
		},
		{
			name: "batch point size",
			setupFn: func(t *testing.T, svc *service) {
				t.Helper()
				// Set point size to smaller size for testing (should result in 3 batches with 3 points, 3 points, and 2 points)
				svc.maxRemoteWritePointSize = 3
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			tt.setupFn(t, svc)

			// Define metadata for two replications
			replications := make([]platform.ID, 2)
			replications[0] = replication1.ID
			replications[1] = replication2.ID

			mocks.durableQueueManager.EXPECT().GetReplications(orgID, id1).Return(replications)

			// Define some points of line protocol, parse string --> []Point
			points, err := models.ParsePointsString(`
cpu,host=0 value=1.1 6000000000
cpu,host=A value=1.2 2000000000
cpu,host=A value=1.3 3000000000
cpu,host=B value=1.3 4000000000
cpu,host=B value=1.3 5000000000
cpu,host=C value=1.3 1000000000
mem,host=C value=1.3 1000000000
disk,host=C value=1.3 1000000000`)
			require.NoError(t, err)

			// Points should successfully write to local TSM.
			mocks.pointWriter.EXPECT().WritePoints(gomock.Any(), orgID, id1, points).Return(nil)

			// Points should successfully be enqueued in the 2 replications associated with the local bucket.
			for _, id := range replications {
				// Check batch 1
				mocks.durableQueueManager.EXPECT().
					EnqueueData(id, gomock.Any(), 3).
					DoAndReturn(func(_ platform.ID, data []byte, numPoints int) error {
						require.Equal(t, 3, numPoints)
						checkCompressedData(t, data, points[:3])
						return nil
					})

				// Check batch 2
				mocks.durableQueueManager.EXPECT().
					EnqueueData(id, gomock.Any(), 3).
					DoAndReturn(func(_ platform.ID, data []byte, numPoints int) error {
						require.Equal(t, 3, numPoints)
						checkCompressedData(t, data, points[3:6])
						return nil
					})

				// Check batch 3
				mocks.durableQueueManager.EXPECT().
					EnqueueData(id, gomock.Any(), 2).
					DoAndReturn(func(_ platform.ID, data []byte, numPoints int) error {
						require.Equal(t, 2, numPoints)
						checkCompressedData(t, data, points[6:])
						return nil
					})
			}

			require.NoError(t, svc.WritePoints(ctx, orgID, id1, points))
		})
	}
}

func TestWritePointsInstanceID(t *testing.T) {
	t.Parallel()

	svc, mocks := newTestService(t)
	svc.instanceID = "hello-edge"
	replications := make([]platform.ID, 2)
	replications[0] = replication1.ID
	replications[1] = replication2.ID

	mocks.durableQueueManager.EXPECT().GetReplications(orgID, id1).Return(replications)

	writePoints, err := models.ParsePointsString(`
cpu,host=0 value=1.1 6000000000
cpu,host=A value=1.2 2000000000
cpu,host=A value=1.3 3000000000
cpu,host=B value=1.3 4000000000
cpu,host=B value=1.3 5000000000
cpu,host=C value=1.3 1000000000
mem,host=C value=1.3 1000000000
disk,host=C value=1.3 1000000000`)
	require.NoError(t, err)

	expectedPoints, err := models.ParsePointsString(`
cpu,host=0,_instance_id=hello-edge value=1.1 6000000000
cpu,host=A,_instance_id=hello-edge value=1.2 2000000000
cpu,host=A,_instance_id=hello-edge value=1.3 3000000000
cpu,host=B,_instance_id=hello-edge value=1.3 4000000000
cpu,host=B,_instance_id=hello-edge value=1.3 5000000000
cpu,host=C,_instance_id=hello-edge value=1.3 1000000000
mem,host=C,_instance_id=hello-edge value=1.3 1000000000
disk,host=C,_instance_id=hello-edge value=1.3 1000000000`)
	require.NoError(t, err)

	// Points should successfully write to local TSM.
	mocks.pointWriter.EXPECT().WritePoints(gomock.Any(), orgID, id1, writePoints).Return(nil)

	// Points should successfully be enqueued in the 2 replications associated with the local bucket.
	for _, id := range replications {
		mocks.durableQueueManager.EXPECT().
			EnqueueData(id, gomock.Any(), len(writePoints)).
			DoAndReturn(func(_ platform.ID, data []byte, numPoints int) error {
				require.Equal(t, len(writePoints), numPoints)
				checkCompressedData(t, data, expectedPoints)
				return nil
			})
	}

	require.NoError(t, svc.WritePoints(ctx, orgID, id1, writePoints))

}

func TestWritePoints_LocalFailure(t *testing.T) {
	t.Parallel()

	svc, mocks := newTestService(t)

	replications := make([]platform.ID, 2)
	replications[0] = replication1.ID
	replications[1] = replication2.ID

	mocks.durableQueueManager.EXPECT().GetReplications(orgID, id1).Return(replications)

	points, err := models.ParsePointsString(`
cpu,host=0 value=1.1 6000000000
cpu,host=A value=1.2 2000000000
cpu,host=A value=1.3 3000000000
cpu,host=B value=1.3 4000000000
cpu,host=B value=1.3 5000000000
cpu,host=C value=1.3 1000000000
mem,host=C value=1.3 1000000000
disk,host=C value=1.3 1000000000`)
	require.NoError(t, err)

	// Points should fail to write to local TSM.
	writeErr := errors.New("O NO")
	mocks.pointWriter.EXPECT().WritePoints(gomock.Any(), orgID, id1, points).Return(writeErr)
	// Don't expect any calls to enqueue points.
	require.Equal(t, writeErr, svc.WritePoints(ctx, orgID, id1, points))
}

func TestOpen(t *testing.T) {
	t.Parallel()

	filter := influxdb.ReplicationListFilter{}

	tests := []struct {
		name            string
		storeErr        error
		queueManagerErr error
		replicationsMap map[platform.ID]*influxdb.TrackedReplication
		list            *influxdb.Replications
	}{
		{
			name: "no error, multiple replications from storage",
			replicationsMap: map[platform.ID]*influxdb.TrackedReplication{
				replication1.ID: {
					MaxQueueSizeBytes: replication1.MaxQueueSizeBytes,
					MaxAgeSeconds:     replication1.MaxAgeSeconds,
					OrgID:             replication1.OrgID,
					LocalBucketID:     replication1.LocalBucketID,
				},
				replication2.ID: {
					MaxQueueSizeBytes: replication2.MaxQueueSizeBytes,
					MaxAgeSeconds:     replication2.MaxAgeSeconds,
					OrgID:             replication2.OrgID,
					LocalBucketID:     replication2.LocalBucketID,
				},
			},
			list: &influxdb.Replications{
				Replications: []influxdb.Replication{replication1, replication2},
			},
		},
		{
			name: "no error, one stored replication",
			replicationsMap: map[platform.ID]*influxdb.TrackedReplication{
				replication1.ID: {
					MaxQueueSizeBytes: replication1.MaxQueueSizeBytes,
					MaxAgeSeconds:     replication1.MaxAgeSeconds,
					OrgID:             replication1.OrgID,
					LocalBucketID:     replication1.LocalBucketID,
				},
			},
			list: &influxdb.Replications{
				Replications: []influxdb.Replication{replication1},
			},
		},
		{
			name:     "store error",
			storeErr: errors.New("store error"),
		},
		{
			name: "queue manager error",
			replicationsMap: map[platform.ID]*influxdb.TrackedReplication{
				replication1.ID: {
					MaxQueueSizeBytes: replication1.MaxQueueSizeBytes,
					MaxAgeSeconds:     replication1.MaxAgeSeconds,
					OrgID:             replication1.OrgID,
					LocalBucketID:     replication1.LocalBucketID,
				},
			},
			list: &influxdb.Replications{
				Replications: []influxdb.Replication{replication1},
			},
			queueManagerErr: errors.New("queue manager error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, mocks := newTestService(t)

			mocks.serviceStore.EXPECT().ListReplications(gomock.Any(), filter).Return(tt.list, tt.storeErr)
			if tt.storeErr == nil {
				mocks.durableQueueManager.EXPECT().StartReplicationQueues(tt.replicationsMap).Return(tt.queueManagerErr)
			}

			var wantErr error
			if tt.storeErr != nil {
				wantErr = tt.storeErr
			} else if tt.queueManagerErr != nil {
				wantErr = tt.queueManagerErr
			}

			err := svc.Open(ctx)
			require.Equal(t, wantErr, err)
		})
	}
}

type mocks struct {
	bucketSvc           *replicationsMock.MockBucketService
	validator           *replicationsMock.MockReplicationValidator
	durableQueueManager *replicationsMock.MockDurableQueueManager
	pointWriter         *replicationsMock.MockPointsWriter
	serviceStore        *replicationsMock.MockServiceStore
}

func checkCompressedData(t *testing.T, data []byte, expectedPoints []models.Point) {
	gzBuf := bytes.NewBuffer(data)
	gzr, err := gzip.NewReader(gzBuf)
	require.NoError(t, err)
	defer gzr.Close()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(gzr)
	require.NoError(t, err)
	require.NoError(t, gzr.Close())

	writtenPoints, err := models.ParsePoints(buf.Bytes())
	require.NoError(t, err)
	require.ElementsMatch(t, writtenPoints, expectedPoints)
}

func newTestService(t *testing.T) (*service, mocks) {
	logger := zaptest.NewLogger(t)

	ctrl := gomock.NewController(t)
	mocks := mocks{
		bucketSvc:           replicationsMock.NewMockBucketService(ctrl),
		validator:           replicationsMock.NewMockReplicationValidator(ctrl),
		durableQueueManager: replicationsMock.NewMockDurableQueueManager(ctrl),
		pointWriter:         replicationsMock.NewMockPointsWriter(ctrl),
		serviceStore:        replicationsMock.NewMockServiceStore(ctrl),
	}
	svc := service{
		store:                   mocks.serviceStore,
		idGenerator:             mock.NewIncrementingIDGenerator(id1),
		bucketService:           mocks.bucketSvc,
		validator:               mocks.validator,
		log:                     logger,
		durableQueueManager:     mocks.durableQueueManager,
		localWriter:             mocks.pointWriter,
		maxRemoteWriteBatchSize: maxRemoteWriteBatchSize,
		maxRemoteWritePointSize: maxRemoteWritePointSize,
	}

	return &svc, mocks
}
