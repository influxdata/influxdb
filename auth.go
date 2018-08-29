package platform

import (
	"context"
	"fmt"
)

// Authorization is a authorization. 🎉
type Authorization struct {
	ID          ID           `json:"id"`
	Token       string       `json:"token"`
	Status      Status       `json:"status"`
	User        string       `json:"user,omitempty"`
	UserID      ID           `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions"`
}

// Allowed returns true if the authorization is active and requested permission level
// exists in the authorization's list of permissions.
func Allowed(req Permission, auth *Authorization) bool {
	if !IsActive(auth) {
		return false
	}

	for _, p := range auth.Permissions {
		if p.Action == req.Action && p.Resource == req.Resource {
			return true
		}
	}
	return false
}

// IsActive returns true if the authorization active.
func IsActive(auth *Authorization) bool {
	return auth.Status == Active
}

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

type action string

const (
	// ReadAction is the action for reading.
	ReadAction action = "read"
	// WriteAction is the action for writing.
	WriteAction action = "write"
	// CreateAction is the action for creating new resources.
	CreateAction action = "create"
	// DeleteAction is the action for deleting an existing resource.
	DeleteAction action = "delete"
)

type resource string

const (
	// UserResource represents the user resource actions can apply to.
	UserResource = resource("user")
	// OrganizationResource represents the org resource actions can apply to.
	OrganizationResource = resource("org")
)

// TaskResource represents the task resource scoped to an organization.
func TaskResource(orgID ID) resource {
	return resource(fmt.Sprintf("org/%s/task", orgID))
}

// BucketResource constructs a bucket resource.
func BucketResource(id ID) resource {
	return resource(fmt.Sprintf("bucket/%s", id))
}

// Permission defines an action and a resource.
type Permission struct {
	Action   action   `json:"action"`
	Resource resource `json:"resource"`
}

func (p Permission) String() string {
	return fmt.Sprintf("%s:%s", p.Action, p.Resource)
}

var (
	// CreateUserPermission is a permission for creating users.
	CreateUserPermission = Permission{
		Action:   CreateAction,
		Resource: UserResource,
	}
	// DeleteUserPermission is a permission for deleting users.
	DeleteUserPermission = Permission{
		Action:   DeleteAction,
		Resource: UserResource,
	}
)

// ReadBucketPermission constructs a permission for reading a bucket.
func ReadBucketPermission(id ID) Permission {
	return Permission{
		Action:   ReadAction,
		Resource: BucketResource(id),
	}
}

// WriteBucketPermission constructs a permission for writing to a bucket.
func WriteBucketPermission(id ID) Permission {
	return Permission{
		Action:   WriteAction,
		Resource: BucketResource(id),
	}
}
