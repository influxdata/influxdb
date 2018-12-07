package platform

import (
	"context"
)

// Authorization is a authorization. 🎉
type Authorization struct {
	ID          ID           `json:"id,omitempty"`
	Token       string       `json:"token"`
	Status      Status       `json:"status"`
	Description string       `json:"description"`
	User        string       `json:"user,omitempty"`
	UserID      ID           `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions,omitempty"`
}

// Allowed returns true if the authorization is active and request permission
// exists in the authorization's list of permissions.
func (a *Authorization) Allowed(p Permission) bool {
	if !a.IsActive() {
		return false
	}

	return allowed(p, a.Permissions)
}

// IsActive is a stub for idpe.
func IsActive(a *Authorization) bool {
	return a.IsActive()
}

// IsActive returns true if the authorization active.
func (a *Authorization) IsActive() bool {
	return a.Status == Active
}

// GetUserID returns the user id.
func (a *Authorization) GetUserID() ID {
	return a.UserID
}

// Kind returns session and is used for auditing.
func (a *Authorization) Kind() string { return "authorization" }

// Identifier returns the authorizations ID and is used for auditing.
func (a *Authorization) Identifier() ID { return a.ID }

// auth service op
const (
	OpFindAuthorizationByID    = "FindAuthorizationByID"
	OpFindAuthorizationByToken = "FindAuthorizationByToken"
	OpFindAuthorizations       = "FindAuthorizations"
	OpCreateAuthorization      = "CreateAuthorization"
	OpSetAuthorizationStatus   = "SetAuthorizationStatus"
	OpDeleteAuthorization      = "DeleteAuthorization"
)

// AuthorizationService represents a service for managing authorization data.
type AuthorizationService interface {
	// Returns a single authorization by ID.
	FindAuthorizationByID(ctx context.Context, id ID) (*Authorization, error)

	// Returns a single authorization by Token.
	FindAuthorizationByToken(ctx context.Context, t string) (*Authorization, error)

	// Returns a list of authorizations that match filter and the total count of matching authorizations.
	// Additional options provide pagination & sorting.
	FindAuthorizations(ctx context.Context, filter AuthorizationFilter, opt ...FindOptions) ([]*Authorization, int, error)

	// Creates a new authorization and sets a.Token and a.UserID with the new identifier.
	CreateAuthorization(ctx context.Context, a *Authorization) error

	// SetAuthorizationStatus updates the status of the authorization. Useful
	// for setting an authorization to inactive or active.
	SetAuthorizationStatus(ctx context.Context, id ID, status Status) error

	// Removes a authorization by token.
	DeleteAuthorization(ctx context.Context, id ID) error
}

// AuthorizationFilter represents a set of filter that restrict the returned results.
type AuthorizationFilter struct {
	Token *string
	ID    *ID

	UserID *ID
	User   *string
}
