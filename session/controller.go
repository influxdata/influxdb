package session

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/feature"
)

var _ influxdb.SessionService = (*ServiceController)(nil)

// ServiceController is a temporary switching mechanism to allow
// a safe rollout of the new session service
type ServiceController struct {
	flagger           feature.Flagger
	oldSessionService influxdb.SessionService
	newSessionService influxdb.SessionService
}

func NewServiceController(flagger feature.Flagger, oldSessionService influxdb.SessionService, newSessionService influxdb.SessionService) *ServiceController {
	return &ServiceController{
		flagger:           flagger,
		oldSessionService: oldSessionService,
		newSessionService: newSessionService,
	}
}

func (s *ServiceController) useNew(ctx context.Context) bool {
	return feature.SessionService().Enabled(ctx, s.flagger)
}

func (s *ServiceController) FindSession(ctx context.Context, key string) (*influxdb.Session, error) {
	if s.useNew(ctx) {
		return s.newSessionService.FindSession(ctx, key)
	}
	return s.oldSessionService.FindSession(ctx, key)
}

func (s *ServiceController) ExpireSession(ctx context.Context, key string) error {
	if s.useNew(ctx) {
		return s.newSessionService.ExpireSession(ctx, key)
	}
	return s.oldSessionService.ExpireSession(ctx, key)
}

func (s *ServiceController) CreateSession(ctx context.Context, user string) (*influxdb.Session, error) {
	if s.useNew(ctx) {
		return s.newSessionService.CreateSession(ctx, user)
	}
	return s.oldSessionService.CreateSession(ctx, user)
}

func (s *ServiceController) RenewSession(ctx context.Context, session *influxdb.Session, newExpiration time.Time) error {
	if s.useNew(ctx) {
		return s.newSessionService.RenewSession(ctx, session, newExpiration)
	}
	return s.oldSessionService.RenewSession(ctx, session, newExpiration)
}
