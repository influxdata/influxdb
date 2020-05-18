package influxdb

import (
	"errors"
	"fmt"
	"path/filepath"
)

var (
	// ErrAuthorizerNotSupported notes that the provided authorizer is not supported for the action you are trying to perform.
	ErrAuthorizerNotSupported = errors.New("your authorizer is not supported, please use *platform.Authorization as authorizer")
	// ErrInvalidResourceType notes that the provided resource is invalid
	ErrInvalidResourceType = errors.New("unknown resource type for permission")
	// ErrInvalidAction notes that the provided action is invalid
	ErrInvalidAction = errors.New("unknown action for permission")
)

// Authorizer will authorize a permission.
type Authorizer interface {
	// PermissionSet returns the PermissionSet associated with the authorizer
	PermissionSet() (PermissionSet, error)

	// ID returns an identifier used for auditing.
	Identifier() ID

	// GetUserID returns the user id.
	GetUserID() ID

	// Kind metadata for auditing.
	Kind() string
}

// PermissionAllowed determines if a permission is allowed.
func PermissionAllowed(perm Permission, ps []Permission) bool {
	for _, p := range ps {
		if p.Matches(perm) {
			return true
		}
	}
	return false
}

// Action is an enum defining all possible resource operations
type Action string

const (
	// ReadAction is the action for reading.
	ReadAction Action = "read" // 1
	// WriteAction is the action for writing.
	WriteAction Action = "write" // 2
)

var actions = []Action{
	ReadAction,  // 1
	WriteAction, // 2
}

// Valid checks if the action is a member of the Action enum
func (a Action) Valid() (err error) {
	switch a {
	case ReadAction: // 1
	case WriteAction: // 2
	default:
		err = ErrInvalidAction
	}

	return err
}

// ResourceType is an enum defining all resource types that have a permission model in platform
type ResourceType string

// Resource is an authorizable resource.
type Resource struct {
	Type  ResourceType `json:"type"`
	ID    *ID          `json:"id,omitempty"`
	OrgID *ID          `json:"orgID,omitempty"`
}

// String stringifies a resource
func (r Resource) String() string {
	if r.OrgID != nil && r.ID != nil {
		return filepath.Join(string(OrgsResourceType), r.OrgID.String(), string(r.Type), r.ID.String())
	}

	if r.OrgID != nil {
		return filepath.Join(string(OrgsResourceType), r.OrgID.String(), string(r.Type))
	}

	if r.ID != nil {
		return filepath.Join(string(r.Type), r.ID.String())
	}

	return string(r.Type)
}

const (
	// AuthorizationsResourceType gives permissions to one or more authorizations.
	AuthorizationsResourceType = ResourceType("authorizations") // 0
	// BucketsResourceType gives permissions to one or more buckets.
	BucketsResourceType = ResourceType("buckets") // 1
	// DashboardsResourceType gives permissions to one or more dashboards.
	DashboardsResourceType = ResourceType("dashboards") // 2
	// OrgsResourceType gives permissions to one or more orgs.
	OrgsResourceType = ResourceType("orgs") // 3
	// SourcesResourceType gives permissions to one or more sources.
	SourcesResourceType = ResourceType("sources") // 4
	// TasksResourceType gives permissions to one or more tasks.
	TasksResourceType = ResourceType("tasks") // 5
	// TelegrafsResourceType type gives permissions to a one or more telegrafs.
	TelegrafsResourceType = ResourceType("telegrafs") // 6
	// UsersResourceType gives permissions to one or more users.
	UsersResourceType = ResourceType("users") // 7
	// VariablesResourceType gives permission to one or more variables.
	VariablesResourceType = ResourceType("variables") // 8
	// ScraperResourceType gives permission to one or more scrapers.
	ScraperResourceType = ResourceType("scrapers") // 9
	// SecretsResourceType gives permission to one or more secrets.
	SecretsResourceType = ResourceType("secrets") // 10
	// LabelsResourceType gives permission to one or more labels.
	LabelsResourceType = ResourceType("labels") // 11
	// ViewsResourceType gives permission to one or more views.
	ViewsResourceType = ResourceType("views") // 12
	// DocumentsResourceType gives permission to one or more documents.
	DocumentsResourceType = ResourceType("documents") // 13
	// NotificationRuleResourceType gives permission to one or more notificationRules.
	NotificationRuleResourceType = ResourceType("notificationRules") // 14
	// NotificationEndpointResourceType gives permission to one or more notificationEndpoints.
	NotificationEndpointResourceType = ResourceType("notificationEndpoints") // 15
	// ChecksResourceType gives permission to one or more Checks.
	ChecksResourceType = ResourceType("checks") // 16
	// DBRPType gives permission to one or more DBRPs.
	DBRPResourceType = ResourceType("dbrp") // 17
)

// AllResourceTypes is the list of all known resource types.
var AllResourceTypes = []ResourceType{
	AuthorizationsResourceType,       // 0
	BucketsResourceType,              // 1
	DashboardsResourceType,           // 2
	OrgsResourceType,                 // 3
	SourcesResourceType,              // 4
	TasksResourceType,                // 5
	TelegrafsResourceType,            // 6
	UsersResourceType,                // 7
	VariablesResourceType,            // 8
	ScraperResourceType,              // 9
	SecretsResourceType,              // 10
	LabelsResourceType,               // 11
	ViewsResourceType,                // 12
	DocumentsResourceType,            // 13
	NotificationRuleResourceType,     // 14
	NotificationEndpointResourceType, // 15
	ChecksResourceType,               // 16
	DBRPResourceType,                 // 17
	// NOTE: when modifying this list, please update the swagger for components.schemas.Permission resource enum.
}

// OrgResourceTypes is the list of all known resource types that belong to an organization.
var OrgResourceTypes = []ResourceType{
	BucketsResourceType,              // 1
	DashboardsResourceType,           // 2
	SourcesResourceType,              // 4
	TasksResourceType,                // 5
	TelegrafsResourceType,            // 6
	UsersResourceType,                // 7
	VariablesResourceType,            // 8
	SecretsResourceType,              // 10
	DocumentsResourceType,            // 13
	NotificationRuleResourceType,     // 14
	NotificationEndpointResourceType, // 15
	ChecksResourceType,               // 16
	DBRPResourceType,                 // 17
}

// Valid checks if the resource type is a member of the ResourceType enum.
func (r Resource) Valid() (err error) {
	return r.Type.Valid()
}

// Valid checks if the resource type is a member of the ResourceType enum.
func (t ResourceType) Valid() (err error) {
	switch t {
	case AuthorizationsResourceType: // 0
	case BucketsResourceType: // 1
	case DashboardsResourceType: // 2
	case OrgsResourceType: // 3
	case TasksResourceType: // 4
	case TelegrafsResourceType: // 5
	case SourcesResourceType: // 6
	case UsersResourceType: // 7
	case VariablesResourceType: // 8
	case ScraperResourceType: // 9
	case SecretsResourceType: // 10
	case LabelsResourceType: // 11
	case ViewsResourceType: // 12
	case DocumentsResourceType: // 13
	case NotificationRuleResourceType: // 14
	case NotificationEndpointResourceType: // 15
	case ChecksResourceType: // 16
	case DBRPResourceType: // 17
	default:
		err = ErrInvalidResourceType
	}

	return err
}

type PermissionSet []Permission

func (ps PermissionSet) Allowed(p Permission) bool {
	return PermissionAllowed(p, ps)
}

// Permission defines an action and a resource.
type Permission struct {
	Action   Action   `json:"action"`
	Resource Resource `json:"resource"`
}

// Matches returns whether or not one permission matches the other.
func (p Permission) Matches(perm Permission) bool {
	if p.Action != perm.Action {
		return false
	}

	if p.Resource.Type != perm.Resource.Type {
		return false
	}

	if p.Resource.OrgID == nil && p.Resource.ID == nil {
		return true
	}

	if p.Resource.OrgID != nil && p.Resource.ID == nil {
		pOrgID := *p.Resource.OrgID
		if perm.Resource.OrgID != nil {
			permOrgID := *perm.Resource.OrgID
			if pOrgID == permOrgID {
				return true
			}
		}
	}

	if p.Resource.ID != nil {
		pID := *p.Resource.ID
		if perm.Resource.ID != nil {
			permID := *perm.Resource.ID
			if pID == permID {
				return true
			}
		}
	}

	return false
}

func (p Permission) String() string {
	return fmt.Sprintf("%s:%s", p.Action, p.Resource)
}

// Valid checks if there the resource and action provided is known.
func (p *Permission) Valid() error {
	if err := p.Resource.Valid(); err != nil {
		return &Error{
			Code: EInvalid,
			Err:  err,
			Msg:  "invalid resource type for permission",
		}
	}

	if err := p.Action.Valid(); err != nil {
		return &Error{
			Code: EInvalid,
			Err:  err,
			Msg:  "invalid action type for permission",
		}
	}

	if p.Resource.OrgID != nil && !p.Resource.OrgID.Valid() {
		return &Error{
			Code: EInvalid,
			Err:  ErrInvalidID,
			Msg:  "invalid org id for permission",
		}
	}

	if p.Resource.ID != nil && !p.Resource.ID.Valid() {
		return &Error{
			Code: EInvalid,
			Err:  ErrInvalidID,
			Msg:  "invalid id for permission",
		}
	}

	return nil
}

// NewPermission returns a permission with provided arguments.
func NewPermission(a Action, rt ResourceType, orgID ID) (*Permission, error) {
	p := &Permission{
		Action: a,
		Resource: Resource{
			Type:  rt,
			OrgID: &orgID,
		},
	}

	return p, p.Valid()
}

// NewResourcePermission returns a permission with provided arguments.
func NewResourcePermission(a Action, rt ResourceType, rid ID) (*Permission, error) {
	p := &Permission{
		Action: a,
		Resource: Resource{
			Type: rt,
			ID:   &rid,
		},
	}

	return p, p.Valid()
}

// NewGlobalPermission constructs a global permission capable of accessing any resource of type rt.
func NewGlobalPermission(a Action, rt ResourceType) (*Permission, error) {
	p := &Permission{
		Action: a,
		Resource: Resource{
			Type: rt,
		},
	}
	return p, p.Valid()
}

// NewPermissionAtID creates a permission with the provided arguments.
func NewPermissionAtID(id ID, a Action, rt ResourceType, orgID ID) (*Permission, error) {
	p := &Permission{
		Action: a,
		Resource: Resource{
			Type:  rt,
			OrgID: &orgID,
			ID:    &id,
		},
	}

	return p, p.Valid()
}

// OperPermissions are the default permissions for those who setup the application.
func OperPermissions() []Permission {
	ps := []Permission{}
	for _, r := range AllResourceTypes {
		for _, a := range actions {
			ps = append(ps, Permission{Action: a, Resource: Resource{Type: r}})
		}
	}

	return ps
}

// ReadAllPermissions represents permission to read all data and metadata.
// Like OperPermissions, but allows read-only users.
func ReadAllPermissions() []Permission {
	ps := make([]Permission, len(AllResourceTypes))
	for i, t := range AllResourceTypes {
		ps[i] = Permission{Action: ReadAction, Resource: Resource{Type: t}}
	}
	return ps
}

// OwnerPermissions are the default permissions for those who own a resource.
func OwnerPermissions(orgID ID) []Permission {
	ps := []Permission{}
	for _, r := range AllResourceTypes {
		for _, a := range actions {
			if r == OrgsResourceType {
				ps = append(ps, Permission{Action: a, Resource: Resource{Type: r, ID: &orgID}})
				continue
			}
			ps = append(ps, Permission{Action: a, Resource: Resource{Type: r, OrgID: &orgID}})
		}
	}
	return ps
}

// MePermissions is the permission to read/write myself.
func MePermissions(userID ID) []Permission {
	ps := []Permission{}
	for _, a := range actions {
		ps = append(ps, Permission{Action: a, Resource: Resource{Type: UsersResourceType, ID: &userID}})
	}

	return ps
}

// MemberPermissions are the default permissions for those who can see a resource.
func MemberPermissions(orgID ID) []Permission {
	ps := []Permission{}
	for _, r := range AllResourceTypes {
		if r == OrgsResourceType {
			ps = append(ps, Permission{Action: ReadAction, Resource: Resource{Type: r, ID: &orgID}})
			continue
		}
		ps = append(ps, Permission{Action: ReadAction, Resource: Resource{Type: r, OrgID: &orgID}})
	}

	return ps
}

// MemberPermissions are the default permissions for those who can see a resource.
func MemberBucketPermission(bucketID ID) Permission {
	return Permission{Action: ReadAction, Resource: Resource{Type: BucketsResourceType, ID: &bucketID}}
}
