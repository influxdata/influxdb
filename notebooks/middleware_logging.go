package notebooks

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

func NewLoggingService(logger *zap.Logger, underlying influxdb.NotebookService) *loggingService {
	return &loggingService{
		logger:     logger,
		underlying: underlying,
	}
}

type loggingService struct {
	logger     *zap.Logger
	underlying influxdb.NotebookService
}

var _ influxdb.NotebookService = (*loggingService)(nil)

func (l loggingService) GetNotebook(ctx context.Context, id platform.ID) (n *influxdb.Notebook, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find notebook by ID", zap.Error(err), dur)
			return
		}
		l.logger.Debug("notebook find by ID", dur)
	}(time.Now())
	return l.underlying.GetNotebook(ctx, id)
}

func (l loggingService) CreateNotebook(ctx context.Context, create *influxdb.NotebookReqBody) (n *influxdb.Notebook, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create notebook", zap.Error(err), dur)
			return
		}
		l.logger.Debug("notebook create", dur)
	}(time.Now())
	return l.underlying.CreateNotebook(ctx, create)
}

func (l loggingService) UpdateNotebook(ctx context.Context, id platform.ID, update *influxdb.NotebookReqBody) (n *influxdb.Notebook, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update notebook", zap.Error(err), dur)
			return
		}
		l.logger.Debug("notebook update", dur)
	}(time.Now())
	return l.underlying.UpdateNotebook(ctx, id, update)
}

func (l loggingService) DeleteNotebook(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete notebook", zap.Error(err), dur)
			return
		}
		l.logger.Debug("notebook delete", dur)
	}(time.Now())
	return l.underlying.DeleteNotebook(ctx, id)
}

func (l loggingService) ListNotebooks(ctx context.Context, filter influxdb.NotebookListFilter) (ns []*influxdb.Notebook, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find notebooks", zap.Error(err), dur)
			return
		}
		l.logger.Debug("notebooks find", dur)
	}(time.Now())
	return l.underlying.ListNotebooks(ctx, filter)
}
