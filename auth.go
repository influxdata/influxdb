package platform

import (
	"context"
	"errors"
	"fmt"
	"regexp"
)

// Authorization is a authorization. 🎉
type Authorization struct {
	Token       string       `json:"token"`
	UserID      ID           `json:"userID,omitempty"`
	Permissions []Permission `json:"permissions"`
}

// AuthorizationService represents a service for managing authorization data.
type AuthorizationService interface {
	// Returns a single authorization by Token.
	FindAuthorizationByToken(ctx context.Context, t string) (*Authorization, error)

	// Returns a list of authorizations that match filter and the total count of matching authorizations.
	// Additional options provide pagination & sorting.
	FindAuthorizations(ctx context.Context, filter AuthorizationFilter, opt ...FindOptions) ([]*Authorization, int, error)

	// Creates a new authorization and sets a.Token with the new identifier.
	CreateAuthorization(ctx context.Context, a *Authorization) error

	// Removes a authorization by token.
	DeleteAuthorization(ctx context.Context, t string) error
}

// AuthorizationFilter represents a set of filter that restrict the returned results.
type AuthorizationFilter struct {
	Token  *string
	UserID *ID
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

// BucketResource constructs a bucket resource.
func BucketResource(t string, b string) resource {
	return resource(fmt.Sprintf("org:%s:bucket:%s", t, b))
}

// Permission defines an action and a resource.
type Permission struct {
	Action   action   `json:"action"`
	Resource resource `json:"resource"`
}

func (p Permission) String() string {
	return fmt.Sprintf("%s:%s", p.Action, p.Resource)
}

// ConstructPermission constructs a permission from a provided action and resource.
func ConstructPermission(a string, r string) (Permission, error) {
	constructedAction := action(a)
	constructedResource := resource(r)
	if !validAction(constructedAction) {
		return Permission{}, errors.New("invalid permission action")
	} else if !validResource(constructedResource) {
		return Permission{}, errors.New("invalid permission resource")
	}
	p := Permission{
		Action:   constructedAction,
		Resource: constructedResource,
	}

	return p, nil
}

func validAction(a action) bool {
	validActions := [4]action{ReadAction, WriteAction, CreateAction, DeleteAction}
	for _, x := range validActions {
		if a == x {
			return true
		}
	}
	return false
}

func validResource(r resource) bool {
	validResources := [2]resource{UserResource, OrganizationResource}
	bucketRegex, _ := regexp.Compile(`org:.+:bucket:.+`)
	for _, x := range validResources {
		if r == x {
			return true
		}
	}
	return bucketRegex.MatchString(string(r))
}

var (
	// CreateUser is a permission for creating users.
	CreateUser = Permission{
		Action:   CreateAction,
		Resource: UserResource,
	}
	// DeleteUser is a permission for deleting users.
	DeleteUser = Permission{
		Action:   DeleteAction,
		Resource: UserResource,
	}
)

// ReadBucket constructs a permission for reading a bucket.
func ReadBucket(o, b string) Permission {
	return Permission{
		Action:   ReadAction,
		Resource: BucketResource(o, b),
	}
}

// WriteBucket constructs a permission for writing to a bucket.
func WriteBucket(o, b string) Permission {
	return Permission{
		Action:   WriteAction,
		Resource: BucketResource(o, b),
	}
}

// Allowed returns true if the permission exists in a list of permissions.
func Allowed(req Permission, ps []Permission) bool {
	for _, p := range ps {
		if p.Action == req.Action && p.Resource == req.Resource {
			return true
		}
	}
	return false
}
