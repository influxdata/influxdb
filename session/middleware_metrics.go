package session

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

// SessionMetrics is a metrics middleware system for the session service
type SessionMetrics struct {
	// RED metrics
	rec *metric.REDClient

	sessionSvc influxdb.SessionService
}

var _ influxdb.SessionService = (*SessionMetrics)(nil)

// NewSessionMetrics creates a new session metrics middleware
func NewSessionMetrics(reg prometheus.Registerer, s influxdb.SessionService) *SessionMetrics {
	return &SessionMetrics{
		rec:        metric.New(reg, "session"),
		sessionSvc: s,
	}
}

// FindSession calls the underlying session service and tracks RED metrics for the call
func (m *SessionMetrics) FindSession(ctx context.Context, key string) (session *influxdb.Session, err error) {
	rec := m.rec.Record("find_session")
	session, err = m.sessionSvc.FindSession(ctx, key)
	return session, rec(err)
}

// ExpireSession calls the underlying session service and tracks RED metrics for the call
func (m *SessionMetrics) ExpireSession(ctx context.Context, key string) (err error) {
	rec := m.rec.Record("expire_session")
	err = m.sessionSvc.ExpireSession(ctx, key)
	return rec(err)
}

// CreateSession calls the underlying session service and tracks RED metrics for the call
func (m *SessionMetrics) CreateSession(ctx context.Context, user string) (s *influxdb.Session, err error) {
	rec := m.rec.Record("create_session")
	s, err = m.sessionSvc.CreateSession(ctx, user)
	return s, rec(err)
}

// RenewSession calls the underlying session service and tracks RED metrics for the call
func (m *SessionMetrics) RenewSession(ctx context.Context, session *influxdb.Session, newExpiration time.Time) (err error) {
	rec := m.rec.Record("renew_session")
	err = m.sessionSvc.RenewSession(ctx, session, newExpiration)
	return rec(err)
}
