package label

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"go.uber.org/zap"
)

type LabelLogger struct {
	logger       *zap.Logger
	labelService influxdb.LabelService
}

func NewLabelLogger(log *zap.Logger, s influxdb.LabelService) *LabelLogger {
	return &LabelLogger{
		logger:       log,
		labelService: s,
	}
}

var _ influxdb.LabelService = (*LabelLogger)(nil)

func (l *LabelLogger) CreateLabel(ctx context.Context, label *influxdb.Label) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create label", zap.Error(err), dur)
			return
		}
		l.logger.Debug("label create", dur)
	}(time.Now())
	return l.labelService.CreateLabel(ctx, label)
}

func (l *LabelLogger) FindLabelByID(ctx context.Context, id platform.ID) (label *influxdb.Label, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to find label with ID %v", id)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("label find by ID", dur)
	}(time.Now())
	return l.labelService.FindLabelByID(ctx, id)
}

func (l *LabelLogger) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) (ls []*influxdb.Label, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find labels matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("labels find", dur)

	}(time.Now())
	return l.labelService.FindLabels(ctx, filter, opt...)
}

func (l *LabelLogger) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) (ls []*influxdb.Label, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find resource labels matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("labels for resource find", dur)

	}(time.Now())
	return l.labelService.FindResourceLabels(ctx, filter)
}

func (l *LabelLogger) UpdateLabel(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (lbl *influxdb.Label, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update label", zap.Error(err), dur)
			return
		}
		l.logger.Debug("label update", dur)

	}(time.Now())
	return l.labelService.UpdateLabel(ctx, id, upd)
}

func (l *LabelLogger) DeleteLabel(ctx context.Context, id platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete label", zap.Error(err), dur)
			return
		}
		l.logger.Debug("label delete", dur)

	}(time.Now())
	return l.labelService.DeleteLabel(ctx, id)
}

func (l *LabelLogger) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create label mapping", zap.Error(err), dur)
			return
		}
		l.logger.Debug("label mapping create", dur)

	}(time.Now())
	return l.labelService.CreateLabelMapping(ctx, m)
}

func (l *LabelLogger) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to delete label mapping", zap.Error(err), dur)
			return
		}
		l.logger.Debug("label mapping delete", dur)

	}(time.Now())
	return l.labelService.DeleteLabelMapping(ctx, m)
}
