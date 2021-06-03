package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

type URMLogger struct {
	logger     *zap.Logger
	urmService influxdb.UserResourceMappingService
}

// NewUrmLogger returns a logging service middleware for the User Resource Mapping Service.
func NewURMLogger(log *zap.Logger, s influxdb.UserResourceMappingService) *URMLogger {
	return &URMLogger{
		logger:     log,
		urmService: s,
	}
}

var _ influxdb.UserResourceMappingService = (*URMLogger)(nil)

func (l *URMLogger) CreateUserResourceMapping(ctx context.Context, u *influxdb.UserResourceMapping) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to create urm", zap.Error(err), dur)
			return
		}
		l.logger.Debug("urm create", dur)
	}(time.Now())
	return l.urmService.CreateUserResourceMapping(ctx, u)
}

func (l *URMLogger) FindUserResourceMappings(ctx context.Context, filter influxdb.UserResourceMappingFilter, opt ...influxdb.FindOptions) (urms []*influxdb.UserResourceMapping, n int, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to find urms matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("urm find", dur)
	}(time.Now())
	return l.urmService.FindUserResourceMappings(ctx, filter, opt...)
}

func (l *URMLogger) DeleteUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to delete urm for resource %v and user %v", resourceID, userID)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("urm delete", dur)
	}(time.Now())
	return l.urmService.DeleteUserResourceMapping(ctx, resourceID, userID)
}
