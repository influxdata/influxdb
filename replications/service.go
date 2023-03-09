package replications

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/replications/internal"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// InfluxDB docs suggest a batch size of 5000 lines for optimal write performance.
// https://docs.influxdata.com/influxdb/v2.1/write-data/best-practices/optimize-writes/
const maxRemoteWritePointSize = 5000

// Uncompressed size (bytes) is used as a secondary limit to prevent network issues and stay below cloud maximum payload
// limitations. 2.5 MB is about 50% of the limit on a basic cloud plan.
// https://docs.influxdata.com/influxdb/cloud/account-management/pricing-plans/#data-limits
const maxRemoteWriteBatchSize = 2500000

func errLocalBucketNotFound(id platform.ID, cause error) error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("local bucket %q not found", id),
		Err:  cause,
	}
}

func NewService(sqlStore *sqlite.SqlStore, bktSvc BucketService, localWriter storage.PointsWriter, log *zap.Logger, enginePath string, instanceID string) (*service, *metrics.ReplicationsMetrics) {
	metrs := metrics.NewReplicationsMetrics()
	store := internal.NewStore(sqlStore)

	return &service{
		store:         store,
		idGenerator:   snowflake.NewIDGenerator(),
		bucketService: bktSvc,
		localWriter:   localWriter,
		validator:     internal.NewValidator(),
		log:           log,
		durableQueueManager: internal.NewDurableQueueManager(
			log,
			filepath.Join(enginePath, "replicationq"),
			metrs,
			store,
		),
		maxRemoteWriteBatchSize: maxRemoteWriteBatchSize,
		maxRemoteWritePointSize: maxRemoteWritePointSize,
		instanceID:              instanceID,
	}, metrs
}

type ReplicationValidator interface {
	ValidateReplication(context.Context, *influxdb.ReplicationHTTPConfig) error
}

type BucketService interface {
	RLock()
	RUnlock()
	FindBucketByID(ctx context.Context, id platform.ID) (*influxdb.Bucket, error)
}

type DurableQueueManager interface {
	InitializeQueue(replicationID platform.ID, maxQueueSizeBytes int64, orgID platform.ID, localBucketID platform.ID, maxAge int64) error
	DeleteQueue(replicationID platform.ID) error
	UpdateMaxQueueSize(replicationID platform.ID, maxQueueSizeBytes int64) error
	CurrentQueueSizes(ids []platform.ID) (map[platform.ID]int64, error)
	RemainingQueueSizes(ids []platform.ID) (map[platform.ID]int64, error)
	StartReplicationQueues(trackedReplications map[platform.ID]*influxdb.TrackedReplication) error
	CloseAll() error
	EnqueueData(replicationID platform.ID, data []byte, numPoints int) error
	GetReplications(orgId platform.ID, localBucketID platform.ID) []platform.ID
}

type ServiceStore interface {
	Lock()
	Unlock()
	ListReplications(context.Context, influxdb.ReplicationListFilter) (*influxdb.Replications, error)
	CreateReplication(context.Context, platform.ID, influxdb.CreateReplicationRequest) (*influxdb.Replication, error)
	GetReplication(context.Context, platform.ID) (*influxdb.Replication, error)
	UpdateReplication(context.Context, platform.ID, influxdb.UpdateReplicationRequest) (*influxdb.Replication, error)
	DeleteReplication(context.Context, platform.ID) error
	PopulateRemoteHTTPConfig(context.Context, platform.ID, *influxdb.ReplicationHTTPConfig) error
	GetFullHTTPConfig(context.Context, platform.ID) (*influxdb.ReplicationHTTPConfig, error)
	DeleteBucketReplications(context.Context, platform.ID) ([]platform.ID, error)
}

type service struct {
	store                   ServiceStore
	idGenerator             platform.IDGenerator
	bucketService           BucketService
	validator               ReplicationValidator
	durableQueueManager     DurableQueueManager
	localWriter             storage.PointsWriter
	log                     *zap.Logger
	maxRemoteWriteBatchSize int
	maxRemoteWritePointSize int
	instanceID              string
}

func (s *service) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (*influxdb.Replications, error) {
	rs, err := s.store.ListReplications(ctx, filter)
	if err != nil {
		return nil, err
	}

	if len(rs.Replications) == 0 {
		return rs, nil
	}

	ids := make([]platform.ID, len(rs.Replications))
	for i := range rs.Replications {
		ids[i] = rs.Replications[i].ID
	}
	sizes, err := s.durableQueueManager.CurrentQueueSizes(ids)
	if err != nil {
		return nil, err
	}
	for i := range rs.Replications {
		rs.Replications[i].CurrentQueueSizeBytes = sizes[rs.Replications[i].ID]
	}
	rsizes, err := s.durableQueueManager.RemainingQueueSizes(ids)
	if err != nil {
		return nil, err
	}
	for i := range rs.Replications {
		rs.Replications[i].RemainingBytesToBeSynced = rsizes[rs.Replications[i].ID]
	}

	return rs, nil
}

func (s *service) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (*influxdb.Replication, error) {
	s.bucketService.RLock()
	defer s.bucketService.RUnlock()

	s.store.Lock()
	defer s.store.Unlock()

	if request.RemoteID == platform.ID(0) && request.RemoteBucketName == "" {
		return nil, fmt.Errorf("please supply one of: remoteBucketID, remoteBucketName")
	}

	if _, err := s.bucketService.FindBucketByID(ctx, request.LocalBucketID); err != nil {
		return nil, errLocalBucketNotFound(request.LocalBucketID, err)
	}

	newID := s.idGenerator.ID()
	if err := s.durableQueueManager.InitializeQueue(newID, request.MaxQueueSizeBytes, request.OrgID, request.LocalBucketID, request.MaxAgeSeconds); err != nil {
		return nil, err
	}

	r, err := s.store.CreateReplication(ctx, newID, request)
	if err != nil {
		if cleanupErr := s.durableQueueManager.DeleteQueue(newID); cleanupErr != nil {
			s.log.Warn("durable queue remaining on disk after initialization failure", zap.Error(cleanupErr), zap.String("id", newID.String()))
		}

		return nil, err
	}

	return r, nil
}

func (s *service) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) error {
	if _, err := s.bucketService.FindBucketByID(ctx, request.LocalBucketID); err != nil {
		return errLocalBucketNotFound(request.LocalBucketID, err)
	}

	config := influxdb.ReplicationHTTPConfig{RemoteBucketID: &request.RemoteBucketID}
	if err := s.store.PopulateRemoteHTTPConfig(ctx, request.RemoteID, &config); err != nil {
		return err
	}

	if err := s.validator.ValidateReplication(ctx, &config); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "replication parameters fail validation",
			Err:  err,
		}
	}
	return nil
}

func (s *service) GetReplication(ctx context.Context, id platform.ID) (*influxdb.Replication, error) {
	r, err := s.store.GetReplication(ctx, id)
	if err != nil {
		return nil, err
	}

	sizes, err := s.durableQueueManager.CurrentQueueSizes([]platform.ID{r.ID})
	if err != nil {
		return nil, err
	}
	r.CurrentQueueSizeBytes = sizes[r.ID]
	rsizes, err := s.durableQueueManager.RemainingQueueSizes([]platform.ID{r.ID})
	if err != nil {
		return nil, err
	}
	r.RemainingBytesToBeSynced = rsizes[r.ID]

	return r, nil
}

func (s *service) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (*influxdb.Replication, error) {
	s.store.Lock()
	defer s.store.Unlock()

	r, err := s.store.UpdateReplication(ctx, id, request)
	if err != nil {
		return nil, err
	}

	if request.MaxQueueSizeBytes != nil {
		if err := s.durableQueueManager.UpdateMaxQueueSize(id, *request.MaxQueueSizeBytes); err != nil {
			s.log.Warn("actual max queue size does not match the max queue size recorded in database", zap.String("id", id.String()))
			return nil, err
		}
	}

	sizes, err := s.durableQueueManager.CurrentQueueSizes([]platform.ID{r.ID})
	if err != nil {
		return nil, err
	}
	r.CurrentQueueSizeBytes = sizes[r.ID]
	rsizes, err := s.durableQueueManager.RemainingQueueSizes([]platform.ID{r.ID})
	if err != nil {
		return nil, err
	}
	r.RemainingBytesToBeSynced = rsizes[r.ID]

	return r, nil
}

func (s *service) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) error {
	baseConfig, err := s.store.GetFullHTTPConfig(ctx, id)
	if err != nil {
		return err
	}
	if request.RemoteBucketID != nil {
		baseConfig.RemoteBucketID = request.RemoteBucketID
	}

	if request.RemoteID != nil {
		if err := s.store.PopulateRemoteHTTPConfig(ctx, *request.RemoteID, baseConfig); err != nil {
			return err
		}
	}

	if err := s.validator.ValidateReplication(ctx, baseConfig); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "validation fails after applying update",
			Err:  err,
		}
	}
	return nil
}

func (s *service) DeleteReplication(ctx context.Context, id platform.ID) error {
	s.store.Lock()
	defer s.store.Unlock()

	if err := s.store.DeleteReplication(ctx, id); err != nil {
		return err
	}

	if err := s.durableQueueManager.DeleteQueue(id); err != nil {
		return err
	}

	return nil
}

func (s *service) DeleteBucketReplications(ctx context.Context, localBucketID platform.ID) error {
	s.store.Lock()
	defer s.store.Unlock()

	deletedIDs, err := s.store.DeleteBucketReplications(ctx, localBucketID)
	if err != nil {
		return err
	}

	errOccurred := false
	deletedStrings := make([]string, 0, len(deletedIDs))
	for _, id := range deletedIDs {
		if err := s.durableQueueManager.DeleteQueue(id); err != nil {
			s.log.Error("durable queue remaining on disk after deletion failure", zap.Error(err), zap.String("id", id.String()))
			errOccurred = true
		}

		deletedStrings = append(deletedStrings, id.String())
	}

	s.log.Debug("deleted replications for local bucket",
		zap.String("bucket_id", localBucketID.String()), zap.Strings("ids", deletedStrings))

	if errOccurred {
		return fmt.Errorf("deleting replications for bucket %q failed, see server logs for details", localBucketID)
	}

	return nil
}

func (s *service) ValidateReplication(ctx context.Context, id platform.ID) error {
	config, err := s.store.GetFullHTTPConfig(ctx, id)
	if err != nil {
		return err
	}
	if err := s.validator.ValidateReplication(ctx, config); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  "replication failed validation",
			Err:  err,
		}
	}
	return nil
}

type batch struct {
	data      *bytes.Buffer
	numPoints int
}

func (s *service) WritePoints(ctx context.Context, orgID platform.ID, bucketID platform.ID, points []models.Point) error {
	replications := s.durableQueueManager.GetReplications(orgID, bucketID)

	// If there are no registered replications, all we need to do is a local write.
	if len(replications) == 0 {
		return s.localWriter.WritePoints(ctx, orgID, bucketID, points)
	}

	if s.instanceID != "" {
		for i := range points {
			points[i].AddTag("_instance_id", s.instanceID)
		}
	}

	// Concurrently...
	var egroup errgroup.Group
	var batches []*batch

	// 1. Write points to local TSM
	egroup.Go(func() error {
		return s.localWriter.WritePoints(ctx, orgID, bucketID, points)
	})
	// 2. Serialize points to gzipped line protocol, to be enqueued for replication if the local write succeeds.
	//    We gzip the LP to take up less room on disk. On the other end of the queue, we can send the gzip data
	//    directly to the remote API without needing to decompress it.
	egroup.Go(func() error {
		// Set up an initial batch
		batches = append(batches, &batch{
			data:      &bytes.Buffer{},
			numPoints: 0,
		})

		currentBatchSize := 0
		gzw := gzip.NewWriter(batches[0].data)

		// Iterate through points and compress in batches
		for count, p := range points {
			// If current point will cause this batch to exceed max size, start a new batch for it first
			if s.startNewBatch(currentBatchSize, p.StringSize(), count) {
				batches = append(batches, &batch{
					data:      &bytes.Buffer{},
					numPoints: 0,
				})

				if err := gzw.Close(); err != nil {
					return err
				}
				currentBatchSize = 0
				gzw = gzip.NewWriter(batches[len(batches)-1].data)
			}

			// Compress point and append to buffer
			if _, err := gzw.Write(append([]byte(p.PrecisionString("ns")), '\n')); err != nil {
				_ = gzw.Close()
				return fmt.Errorf("failed to serialize points for replication: %w", err)
			}

			batches[len(batches)-1].numPoints += 1
			currentBatchSize += p.StringSize()
		}
		if err := gzw.Close(); err != nil {
			return err
		}
		return nil
	})

	if err := egroup.Wait(); err != nil {
		return err
	}

	// Enqueue the data into all registered replications.
	var wg sync.WaitGroup
	wg.Add(len(replications))

	for _, id := range replications {
		go func(id platform.ID) {
			defer wg.Done()

			// Iterate through batches and enqueue each
			for _, batch := range batches {
				if err := s.durableQueueManager.EnqueueData(id, batch.data.Bytes(), batch.numPoints); err != nil {
					s.log.Error("Failed to enqueue points for replication", zap.String("id", id.String()), zap.Error(err))
				}
			}
		}(id)
	}
	wg.Wait()

	return nil
}

func (s *service) Open(ctx context.Context) error {
	trackedReplications, err := s.store.ListReplications(ctx, influxdb.ReplicationListFilter{})
	if err != nil {
		return err
	}

	trackedReplicationsMap := make(map[platform.ID]*influxdb.TrackedReplication)
	for _, r := range trackedReplications.Replications {
		trackedReplicationsMap[r.ID] = &influxdb.TrackedReplication{
			MaxQueueSizeBytes: r.MaxQueueSizeBytes,
			MaxAgeSeconds:     r.MaxAgeSeconds,
			OrgID:             r.OrgID,
			LocalBucketID:     r.LocalBucketID,
		}
	}

	// Queue manager completes startup tasks
	if err := s.durableQueueManager.StartReplicationQueues(trackedReplicationsMap); err != nil {
		return err
	}
	return nil
}

func (s *service) Close() error {
	if err := s.durableQueueManager.CloseAll(); err != nil {
		return err
	}
	return nil
}

func (s *service) startNewBatch(currentSize, nextSize, pointCount int) bool {
	return currentSize+nextSize > s.maxRemoteWriteBatchSize ||
		pointCount > 0 && pointCount%s.maxRemoteWritePointSize == 0
}
