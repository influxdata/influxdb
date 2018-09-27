package mock

import (
	"context"
	"fmt"

	"github.com/influxdata/platform"
)

// SessionService is a mock implementation of a retention.SessionService, which
// also makes it a suitable mock to use wherever an platform.SessionService is required.
type SessionService struct {
	FindSessionFn   func(context.Context, string) (*platform.Session, error)
	ExpireSessionFn func(context.Context, string) error
	CreateSessionFn func(context.Context, string) (*platform.Session, error)
}

// NewSessionService returns a mock SessionService where its methods will return
// zero values.
func NewSessionService() *SessionService {
	return &SessionService{
		FindSessionFn:   func(context.Context, string) (*platform.Session, error) { return nil, fmt.Errorf("mock session") },
		CreateSessionFn: func(context.Context, string) (*platform.Session, error) { return nil, fmt.Errorf("mock session") },
		ExpireSessionFn: func(context.Context, string) error { return fmt.Errorf("mock session") },
	}
}

// FindSession returns the session found at the provided key.
func (s *SessionService) FindSession(ctx context.Context, key string) (*platform.Session, error) {
	return s.FindSessionFn(ctx, key)
}

// CreateSession creates a sesion for a user with the users maximal privileges.
func (s *SessionService) CreateSession(ctx context.Context, user string) (*platform.Session, error) {
	return s.CreateSessionFn(ctx, user)
}

// ExpireSession exires the session provided at key.
func (s *SessionService) ExpireSession(ctx context.Context, key string) error {
	return s.ExpireSessionFn(ctx, key)
}
