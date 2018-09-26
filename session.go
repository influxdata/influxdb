package platform

import (
	"context"
	"fmt"
	"time"
)

// Session is a user session.
type Session struct {
	// ID is only required for auditing purposes.
	ID          ID           `json:"id"`
	Key         string       `json:"key"`
	CreatedAt   time.Time    `json:"createdAt"`
	ExpiresAt   time.Time    `json:"expiresAt"`
	UserID      ID           `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions,omitempty"`
}

func (s *Session) Expired() error {
	if time.Now().After(s.ExpiresAt) {
		return fmt.Errorf("session has expired")
	}

	return nil
}

// SessionService represents a service for managing user sessions.
type SessionService interface {
	FindSession(ctx context.Context, key string) (*Session, error)
	ExpireSession(ctx context.Context, key string) error
	CreateSession(ctx context.Context, user string) (*Session, error)
}
