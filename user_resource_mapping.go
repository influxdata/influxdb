package influxdb

import (
	"context"
	"errors"
)

var (
	// ErrInvalidUserType notes that the provided UserType is invalid
	ErrInvalidUserType = errors.New("unknown user type")
	// ErrUserIDRequired notes that the ID was not provided
	ErrUserIDRequired = errors.New("user id is required")
	// ErrResourceIDRequired notes that the provided ID was not provided
	ErrResourceIDRequired = errors.New("resource id is required")
)

// UserType can either be owner or member.
type UserType string

const (
	// Owner can read and write to a resource
	Owner UserType = "owner" // 1
	// Member can read from a resource.
	Member UserType = "member" // 2
)

// Valid checks if the UserType is a member of the UserType enum
func (ut UserType) Valid() (err error) {
	switch ut {
	case Owner: // 1
	case Member: // 2
	default:
		err = ErrInvalidUserType
	}

	return err
}

// UserResourceMappingService maps the relationships between users and resources.
type UserResourceMappingService interface {
	// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
	FindUserResourceMappings(ctx context.Context, filter UserResourceMappingFilter, opt ...FindOptions) ([]*UserResourceMapping, int, error)

	// CreateUserResourceMapping creates a user resource mapping.
	CreateUserResourceMapping(ctx context.Context, m *UserResourceMapping) error

	// DeleteUserResourceMapping deletes a user resource mapping.
	DeleteUserResourceMapping(ctx context.Context, resourceID ID, userID ID) error
}

// UserResourceMapping represents a mapping of a resource to its user.
type UserResourceMapping struct {
	UserID       ID           `json:"userID"`
	UserType     UserType     `json:"userType"`
	ResourceType ResourceType `json:"resourceType"`
	ResourceID   ID           `json:"resourceID"`
}

// Validate reports any validation errors for the mapping.
func (m UserResourceMapping) Validate() error {
	if !m.ResourceID.Valid() {
		return ErrResourceIDRequired
	}

	if !m.UserID.Valid() {
		return ErrUserIDRequired
	}

	if err := m.UserType.Valid(); err != nil {
		return err
	}

	if err := m.ResourceType.Valid(); err != nil {
		return err
	}

	return nil
}

// UserResourceMappingFilter represents a set of filters that restrict the returned results.
type UserResourceMappingFilter struct {
	ResourceID   ID
	ResourceType ResourceType
	UserID       ID
	UserType     UserType
}

func (m *UserResourceMapping) ownerPerms() ([]Permission, error) {
	ps := []Permission{}
	// TODO(desa): how to grant access to specific resources.

	if m.ResourceType == OrgsResourceType {
		ps = append(ps, OwnerPermissions(m.ResourceID)...)
	}

	return ps, nil
}

func (m *UserResourceMapping) memberPerms() ([]Permission, error) {
	ps := []Permission{}
	// TODO(desa): how to grant access to specific resources.

	if m.ResourceType == OrgsResourceType {
		ps = append(ps, MemberPermissions(m.ResourceID)...)
	}

	return ps, nil
}

// ToPermissions converts a user resource mapping into a set of permissions.
func (m *UserResourceMapping) ToPermissions() ([]Permission, error) {
	switch m.UserType {
	case Owner:
		return m.ownerPerms()
	case Member:
		return m.memberPerms()
	default:
		return nil, ErrInvalidUserType
	}
}
