package replications

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

func NewLoggingService(logger *zap.Logger, underlying influxdb.ReplicationService) *loggingService {
	return &loggingService{
		logger:     logger,
		underlying: underlying,
	}
}

type loggingService struct {
	logger     *zap.Logger
	underlying influxdb.ReplicationService
}

var _ influxdb.ReplicationService = (*loggingService)(nil)

func (l loggingService) ListReplications(ctx context.Context, filter influxdb.ReplicationListFilter) (rs *influxdb.Replications, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find replications", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replications find", dur)
	}(time.Now())
	return l.underlying.ListReplications(ctx, filter)
}

func (l loggingService) CreateReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (r *influxdb.Replication, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create replication", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication create", dur)
	}(time.Now())
	return l.underlying.CreateReplication(ctx, request)
}

func (l loggingService) ValidateNewReplication(ctx context.Context, request influxdb.CreateReplicationRequest) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate replication create", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication validate create", dur)
	}(time.Now())
	return l.underlying.ValidateNewReplication(ctx, request)
}

func (l loggingService) GetReplication(ctx context.Context, id platform.ID) (r *influxdb.Replication, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find replication by ID", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication find by ID", dur)
	}(time.Now())
	return l.underlying.GetReplication(ctx, id)
}

func (l loggingService) UpdateReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (r *influxdb.Replication, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update replication", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication update", dur)
	}(time.Now())
	return l.underlying.UpdateReplication(ctx, id, request)
}

func (l loggingService) ValidateUpdatedReplication(ctx context.Context, id platform.ID, request influxdb.UpdateReplicationRequest) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate replication update", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication validate update", dur)
	}(time.Now())
	return l.underlying.ValidateUpdatedReplication(ctx, id, request)
}

func (l loggingService) DeleteReplication(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete replication", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication delete", dur)
	}(time.Now())
	return l.underlying.DeleteReplication(ctx, id)
}

func (l loggingService) ValidateReplication(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate replication", zap.Error(err), dur)
			return
		}
		l.logger.Debug("replication validate", dur)
	}(time.Now())
	return l.underlying.ValidateReplication(ctx, id)
}
