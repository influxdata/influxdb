package influxdb

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// ErrSessionNotFound is the error messages for a missing sessions.
const ErrSessionNotFound = "session not found"

// ErrSessionExpired is the error message for expired sessions.
const ErrSessionExpired = "session has expired"

// RenewSessionTime is the the time to extend session, currently set to 5min.
var RenewSessionTime = time.Duration(time.Second * 300)

// DefaultSessionLength is the default session length on initial creation.
var DefaultSessionLength = time.Hour

var (
	// OpFindSession represents the operation that looks for sessions.
	OpFindSession = "FindSession"
	// OpExpireSession represents the operation that expires sessions.
	OpExpireSession = "ExpireSession"
	// OpCreateSession represents the operation that creates a session for a given user.
	OpCreateSession = "CreateSession"
	// OpRenewSession = "RenewSession"
	OpRenewSession = "RenewSession"
)

// SessionAuthorizationKind defines the type of authorizer
const SessionAuthorizationKind = "session"

// Session is a user session.
type Session struct {
	// ID is only required for auditing purposes.
	ID          platform.ID  `json:"id"`
	Key         string       `json:"key"`
	CreatedAt   time.Time    `json:"createdAt"`
	ExpiresAt   time.Time    `json:"expiresAt"`
	UserID      platform.ID  `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions,omitempty"`
}

// Expired returns an error if the session is expired.
func (s *Session) Expired() error {
	if time.Now().After(s.ExpiresAt) {
		return &errors.Error{
			Code: errors.EForbidden,
			Msg:  ErrSessionExpired,
		}
	}

	return nil
}

// PermissionSet returns the set of permissions associated with the session.
func (s *Session) PermissionSet() (PermissionSet, error) {
	if err := s.Expired(); err != nil {
		return nil, &errors.Error{
			Code: errors.EUnauthorized,
			Err:  err,
		}
	}

	return s.Permissions, nil
}

// Kind returns session and is used for auditing.
func (s *Session) Kind() string { return SessionAuthorizationKind }

// Identifier returns the sessions ID and is used for auditing.
func (s *Session) Identifier() platform.ID { return s.ID }

// GetUserID returns the user id.
func (s *Session) GetUserID() platform.ID {
	return s.UserID
}

// EphemeralAuth generates an Authorization that is not stored
// but at the user's max privs.
func (s *Session) EphemeralAuth(orgID platform.ID) *Authorization {
	return &Authorization{
		ID:          s.ID,
		OrgID:       orgID,
		Status:      Active,
		UserID:      s.UserID,
		Permissions: s.Permissions,
	}
}

// SessionService represents a service for managing user sessions.
type SessionService interface {
	FindSession(ctx context.Context, key string) (*Session, error)
	ExpireSession(ctx context.Context, key string) error
	CreateSession(ctx context.Context, user string) (*Session, error)
	// TODO: update RenewSession to take a ID instead of a session.
	// By taking a session object it could be confused to update more things about the session
	RenewSession(ctx context.Context, session *Session, newExpiration time.Time) error
}
