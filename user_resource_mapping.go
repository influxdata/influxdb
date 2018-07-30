package platform

import (
	"context"
	"errors"
)

type UserType string

const (
	Owner  UserType = "owner"
	Member UserType = "member"
)

// UserResourceMappingService maps the relationships between users and resources
type UserResourceMappingService interface {
	CreateUserResourceMapping(ctx context.Context, m *UserResourceMapping) error
	DeleteUserResourceMapping(ctx context.Context, resourceID ID, userID ID) error
}

// UserResourceMapping represents a mapping of a resource to its user
type UserResourceMapping struct {
	ResourceID ID       `json:"resource_id"`
	UserID     ID       `json:"user_id"`
	UserType   UserType `json:"user_type"`
}

// Validate reports any validation errors for the mapping.
func (m UserResourceMapping) Validate() error {
	if len(m.ResourceID) == 0 {
		return errors.New("ResourceID is required")
	}
	if len(m.UserID) == 0 {
		return errors.New("UserID is required")
	}
	if m.UserType != Owner && m.UserType != Member {
		return errors.New("A valid user type is required")
	}
	return nil
}

// UserResourceMapping represents a set of filters that restrict the returned results.
type UserResourceMappingFilter struct {
	ResourceID ID
	UserID     ID
}
