package platform

import (
	"errors"
	"fmt"
)

var (
	// ErrAuthorizerNotSupported notes that the provided authorizer is not supported for the action you are trying to perform.
	ErrAuthorizerNotSupported = errors.New("your authorizer is not supported, please use *platform.Authorization as authorizer")
)

// Authorizer will authorize a permission.
type Authorizer interface {
	// Allowed returns true is the associated permission is allowed by the authorizer
	Allowed(p Permission) bool

	// ID returns an identifier used for auditing.
	Identifier() ID

	// GetUserID returns the user id.
	GetUserID() ID

	// Kind metadata for auditing.
	Kind() string
}

func allowed(p Permission, ps []Permission) bool {
	for _, perm := range ps {
		if perm.Action == p.Action && perm.Resource == p.Resource {
			return true
		}
	}
	return false
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
