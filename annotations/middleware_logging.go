package annotations

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

func NewLoggingService(logger *zap.Logger, underlying influxdb.AnnotationService) *loggingService {
	return &loggingService{
		logger:     logger,
		underlying: underlying,
	}
}

type loggingService struct {
	logger     *zap.Logger
	underlying influxdb.AnnotationService
}

var _ influxdb.AnnotationService = (*loggingService)(nil)

func (l loggingService) CreateAnnotations(ctx context.Context, orgID platform.ID, create []influxdb.AnnotationCreate) (an []influxdb.AnnotationEvent, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create annotations", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotations create", dur)
	}(time.Now())
	return l.underlying.CreateAnnotations(ctx, orgID, create)
}

func (l loggingService) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) (an []influxdb.StoredAnnotation, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find annotations", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotations find", dur)
	}(time.Now())
	return l.underlying.ListAnnotations(ctx, orgID, filter)
}

func (l loggingService) GetAnnotation(ctx context.Context, id platform.ID) (an *influxdb.StoredAnnotation, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find annotation by ID", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotation find by ID", dur)
	}(time.Now())
	return l.underlying.GetAnnotation(ctx, id)
}

func (l loggingService) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete annotations", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotations delete", dur)
	}(time.Now())
	return l.underlying.DeleteAnnotations(ctx, orgID, delete)
}

func (l loggingService) DeleteAnnotation(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete annotation", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotation delete", dur)
	}(time.Now())
	return l.underlying.DeleteAnnotation(ctx, id)
}

func (l loggingService) UpdateAnnotation(ctx context.Context, id platform.ID, update influxdb.AnnotationCreate) (an *influxdb.AnnotationEvent, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update annotation", zap.Error(err), dur)
			return
		}
		l.logger.Debug("annotation update", dur)
	}(time.Now())
	return l.underlying.UpdateAnnotation(ctx, id, update)
}

func (l loggingService) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) (stm []influxdb.StoredStream, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find streams", zap.Error(err), dur)
			return
		}
		l.logger.Debug("streams find", dur)
	}(time.Now())
	return l.underlying.ListStreams(ctx, orgID, filter)
}

func (l loggingService) CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream influxdb.Stream) (stm *influxdb.ReadStream, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create or update stream", zap.Error(err), dur)
			return
		}
		l.logger.Debug("stream create or update", dur)
	}(time.Now())
	return l.underlying.CreateOrUpdateStream(ctx, orgID, stream)
}

func (l loggingService) UpdateStream(ctx context.Context, id platform.ID, stream influxdb.Stream) (stm *influxdb.ReadStream, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update stream", zap.Error(err), dur)
			return
		}
		l.logger.Debug("stream update", dur)
	}(time.Now())
	return l.underlying.UpdateStream(ctx, id, stream)
}

func (l loggingService) GetStream(ctx context.Context, id platform.ID) (stm *influxdb.StoredStream, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find stream by ID", zap.Error(err), dur)
			return
		}
		l.logger.Debug("stream find by ID", dur)
	}(time.Now())
	return l.underlying.GetStream(ctx, id)
}

func (l loggingService) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete streams", zap.Error(err), dur)
			return
		}
		l.logger.Debug("streams delete", dur)
	}(time.Now())
	return l.underlying.DeleteStreams(ctx, orgID, delete)
}

func (l loggingService) DeleteStreamByID(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete stream", zap.Error(err), dur)
			return
		}
		l.logger.Debug("stream delete", dur)
	}(time.Now())
	return l.underlying.DeleteStreamByID(ctx, id)
}
