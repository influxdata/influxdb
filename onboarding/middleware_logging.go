package onboarding

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

// NewLoggingService returns a logging service middleware for the Bucket Service.
func NewLoggingService(log *zap.Logger, s influxdb.OnboardingService) *loggingService {
	return &loggingService{
		logger:     log,
		underlying: s,
	}
}

type loggingService struct {
	logger     *zap.Logger
	underlying influxdb.OnboardingService
}

var _ influxdb.OnboardingService = (*loggingService)(nil)

func (l *loggingService) IsOnboarding(ctx context.Context) (available bool, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Error("failed to check onboarding", zap.Error(err), dur)
			return
		}
		l.logger.Debug("is onboarding", dur)
	}(time.Now())
	return l.underlying.IsOnboarding(ctx)
}

func (l *loggingService) OnboardInitialUser(ctx context.Context, req *influxdb.OnboardingRequest) (res *influxdb.OnboardingResults, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to onboard user %s", req.User)
			l.logger.Error(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("onboard initial user", dur)
	}(time.Now())
	return l.underlying.OnboardInitialUser(ctx, req)
}
