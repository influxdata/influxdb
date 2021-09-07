package transport

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

func newLoggingService(logger *zap.Logger, underlying RemoteConnectionService) *loggingService {
	return &loggingService{
		logger:     logger,
		underlying: underlying,
	}
}

type loggingService struct {
	logger     *zap.Logger
	underlying RemoteConnectionService
}

var _ RemoteConnectionService = (*loggingService)(nil)

func (l loggingService) ListRemoteConnections(ctx context.Context, filter influxdb.RemoteConnectionListFilter) (cs *influxdb.RemoteConnections, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find remotes", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remotes find", dur)
	}(time.Now())
	return l.underlying.ListRemoteConnections(ctx, filter)
}

func (l loggingService) CreateRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (r *influxdb.RemoteConnection, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create remote", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote create", dur)
	}(time.Now())
	return l.underlying.CreateRemoteConnection(ctx, request)
}

func (l loggingService) ValidateNewRemoteConnection(ctx context.Context, request influxdb.CreateRemoteConnectionRequest) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate remote create", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote validate create", dur)
	}(time.Now())
	return l.underlying.ValidateNewRemoteConnection(ctx, request)
}

func (l loggingService) GetRemoteConnection(ctx context.Context, id platform.ID) (r *influxdb.RemoteConnection, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find remote by ID", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote find by ID", dur)
	}(time.Now())
	return l.underlying.GetRemoteConnection(ctx, id)
}

func (l loggingService) UpdateRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (r *influxdb.RemoteConnection, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update remote", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote update", dur)
	}(time.Now())
	return l.underlying.UpdateRemoteConnection(ctx, id, request)
}

func (l loggingService) ValidateUpdatedRemoteConnection(ctx context.Context, id platform.ID, request influxdb.UpdateRemoteConnectionRequest) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate remote update", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote validate update", dur)
	}(time.Now())
	return l.underlying.ValidateUpdatedRemoteConnection(ctx, id, request)
}

func (l loggingService) DeleteRemoteConnection(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete remote", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote delete", dur)
	}(time.Now())
	return l.underlying.DeleteRemoteConnection(ctx, id)
}

func (l loggingService) ValidateRemoteConnection(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to validate remote", zap.Error(err), dur)
			return
		}
		l.logger.Debug("remote validate", dur)
	}(time.Now())
	return l.underlying.ValidateRemoteConnection(ctx, id)
}
