package session

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

// SessionLogger is a logger service middleware for sessions
type SessionLogger struct {
	logger         *zap.Logger
	sessionService influxdb.SessionService
}

var _ influxdb.SessionService = (*SessionLogger)(nil)

// NewSessionLogger returns a logging service middleware for the User Service.
func NewSessionLogger(log *zap.Logger, s influxdb.SessionService) *SessionLogger {
	return &SessionLogger{
		logger:         log,
		sessionService: s,
	}
}

// FindSession calls the underlying session service and logs the results of the request
func (l *SessionLogger) FindSession(ctx context.Context, key string) (session *influxdb.Session, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to session find", zap.Error(err), dur)
			return
		}
		l.logger.Debug("session find", dur)
	}(time.Now())
	return l.sessionService.FindSession(ctx, key)

}

// ExpireSession calls the underlying session service and logs the results of the request
func (l *SessionLogger) ExpireSession(ctx context.Context, key string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to session expire", zap.Error(err), dur)
			return
		}
		l.logger.Debug("session expire", dur)
	}(time.Now())
	return l.sessionService.ExpireSession(ctx, key)

}

// CreateSession calls the underlying session service and logs the results of the request
func (l *SessionLogger) CreateSession(ctx context.Context, user string) (s *influxdb.Session, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to session create", zap.Error(err), dur)
			return
		}
		l.logger.Debug("session create", dur)
	}(time.Now())
	return l.sessionService.CreateSession(ctx, user)

}

// RenewSession calls the underlying session service and logs the results of the request
func (l *SessionLogger) RenewSession(ctx context.Context, session *influxdb.Session, newExpiration time.Time) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to session renew", zap.Error(err), dur)
			return
		}
		l.logger.Debug("session renew", dur)
	}(time.Now())
	return l.sessionService.RenewSession(ctx, session, newExpiration)

}
