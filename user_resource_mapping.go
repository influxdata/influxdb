package influxdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

var (
	// ErrInvalidUserType notes that the provided UserType is invalid
	ErrInvalidUserType = errors.New("unknown user type")
	// ErrInvalidMappingType notes that the provided MappingType is invalid
	ErrInvalidMappingType = errors.New("unknown mapping type")
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

type MappingType uint8

const (
	UserMappingType = 0
	OrgMappingType  = 1
)

func (mt MappingType) Valid() error {
	switch mt {
	case UserMappingType, OrgMappingType:
		return nil
	}

	return ErrInvalidMappingType
}

func (mt MappingType) String() string {
	switch mt {
	case UserMappingType:
		return "user"
	case OrgMappingType:
		return "org"
	}

	return "unknown"
}

func (mt MappingType) MarshalJSON() ([]byte, error) {
	return json.Marshal(mt.String())
}

func (mt *MappingType) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	switch s {
	case "user":
		*mt = UserMappingType
		return nil
	case "org":
		*mt = OrgMappingType
		return nil
	}

	return ErrInvalidMappingType
}

// UserResourceMappingService maps the relationships between users and resources.
type UserResourceMappingService interface {
	// FindUserResourceMappings returns a list of UserResourceMappings that match filter and the total count of matching mappings.
	FindUserResourceMappings(ctx context.Context, filter UserResourceMappingFilter, opt ...FindOptions) ([]*UserResourceMapping, int, error)

	// CreateUserResourceMapping creates a user resource mapping.
	CreateUserResourceMapping(ctx context.Context, m *UserResourceMapping) error

	// DeleteUserResourceMapping deletes a user resource mapping.
	DeleteUserResourceMapping(ctx context.Context, resourceID, userID platform.ID) error
}

// UserResourceMapping represents a mapping of a resource to its user.
type UserResourceMapping struct {
	UserID       platform.ID  `json:"userID"`
	UserType     UserType     `json:"userType"`
	MappingType  MappingType  `json:"mappingType"`
	ResourceType ResourceType `json:"resourceType"`
	ResourceID   platform.ID  `json:"resourceID"`
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

	if err := m.MappingType.Valid(); err != nil {
		return err
	}

	if err := m.ResourceType.Valid(); err != nil {
		return err
	}

	return nil
}

// UserResourceMappingFilter represents a set of filters that restrict the returned results.
type UserResourceMappingFilter struct {
	ResourceID   platform.ID
	ResourceType ResourceType
	UserID       platform.ID
	UserType     UserType
}

func (m *UserResourceMapping) ownerPerms() ([]Permission, error) {
	if m.ResourceType == OrgsResourceType {
		return OwnerPermissions(m.ResourceID), nil
	}

	ps := []Permission{
		// TODO: Uncomment these once the URM system is no longer being used for find lookups for:
		// 	Telegraf
		// 	DashBoard
		// 	notification rule
		// 	notification endpoint
		// Permission{
		// 	Action: ReadAction,
		// 	Resource: Resource{
		// 		Type: m.ResourceType,
		// 		ID:   &m.ResourceID,
		// 	},
		// },
		// Permission{
		// 	Action: WriteAction,
		// 	Resource: Resource{
		// 		Type: m.ResourceType,
		// 		ID:   &m.ResourceID,
		// 	},
		// },
	}
	return ps, nil
}

func (m *UserResourceMapping) memberPerms() ([]Permission, error) {
	if m.ResourceType == OrgsResourceType {
		return MemberPermissions(m.ResourceID), nil
	}

	if m.ResourceType == BucketsResourceType {
		return []Permission{MemberBucketPermission(m.ResourceID)}, nil
	}

	ps := []Permission{
		// TODO: Uncomment these once the URM system is no longer being used for find lookups for:
		// 	Telegraf
		// 	DashBoard
		// 	notification rule
		// 	notification endpoint
		// Permission{
		// 	Action: ReadAction,
		// 	Resource: Resource{
		// 		Type: m.ResourceType,
		// 		ID:   &m.ResourceID,
		// 	},
		// },
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
