package influxdb

import (
	"errors"
	"fmt"
)

var (
	// ErrAuthorizerNotSupported notes that the provided authorizer is not supported for the action you are trying to perform.
	ErrAuthorizerNotSupported = errors.New("your authorizer is not supported, please use *platform.Authorization as authorizer")
	// ErrInvalidResource notes that the provided resource is invalid
	ErrInvalidResource = errors.New("unknown resource for permission")
	// ErrInvalidAction notes that the provided action is invalid
	ErrInvalidAction = errors.New("unknown action for permission")
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

// PermissionAllowed determines if a permission is allowed.
func PermissionAllowed(p Permission, ps []Permission) bool {
	if !p.OrgID.Valid() {
		return false
	}

	pID := ID(0)
	if p.ID != nil {
		pID = *p.ID
		if !pID.Valid() {
			return false
		}
	}

	for _, perm := range ps {
		if perm.ID == nil {
			if perm.Action == p.Action && perm.Resource == p.Resource && perm.OrgID == p.OrgID {
				return true
			}
		} else {
			if *perm.ID == pID && perm.Action == p.Action && perm.Resource == p.Resource && perm.OrgID == p.OrgID {
				return true
			}
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

// Resource is an enum defining all resources that have a permission model in platform
type Resource string

const (
	// AuthorizationsResource gives permissions to one or more authorizations.
	AuthorizationsResource = Resource("authorizations") // 0
	// BucketsResource gives permissions to one or more buckets.
	BucketsResource = Resource("buckets") // 1
	// DashboardsResource gives permissions to one or more dashboards.
	DashboardsResource = Resource("dashboards") // 2
	// OrgsResource gives permissions to one or more orgs.
	OrgsResource = Resource("orgs") // 3
	// SourcesResource gives permissions to one or more sources.
	SourcesResource = Resource("sources") // 4
	// TasksResource gives permissions to one or more tasks.
	TasksResource = Resource("tasks") // 5
	// TelegrafsResource type gives permissions to a one or more telegrafs.
	TelegrafsResource = Resource("telegrafs") // 6
	// UsersResource gives permissions to one or more users.
	UsersResource = Resource("users") // 7
)

// AllResources is the list of all known resource types.
var AllResources = []Resource{
	AuthorizationsResource, // 0
	BucketsResource,        // 1
	DashboardsResource,     // 2
	OrgsResource,           // 3
	SourcesResource,        // 4
	TasksResource,          // 5
	TelegrafsResource,      // 6
	UsersResource,          // 7
}

// OrgResources is the list of all known resource types that belong to an organization.
var OrgResources = []Resource{
	BucketsResource,    // 1
	DashboardsResource, // 2
	SourcesResource,    // 4
	TasksResource,      // 5
	TelegrafsResource,  // 6
	UsersResource,      // 7
}

// Valid checks if the resource is a member of the Resource enum.
func (r Resource) Valid() (err error) {
	switch r {
	case AuthorizationsResource: // 0
	case BucketsResource: // 1
	case DashboardsResource: // 2
	case OrgsResource: // 3
	case TasksResource: // 4
	case TelegrafsResource: // 5
	case SourcesResource: // 6
	case UsersResource: //7
	default:
		err = ErrInvalidResource
	}

	return err
}

// Permission defines an action and a resource.
type Permission struct {
	Action   Action   `json:"action"`
	Resource Resource `json:"resource"`
	OrgID    ID       `json:"orgID"`
	ID       *ID      `json:"id,omitempty"`
}

func (p Permission) String() string {
	str := fmt.Sprintf("%s:orgs/%s/%s", p.Action, p.OrgID, p.Resource)
	if p.ID != nil {
		str += fmt.Sprintf("/%s", (*p.ID).String())
	}

	return str
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

	if !p.OrgID.Valid() {
		return &Error{
			Code: EInvalid,
			Err:  ErrInvalidID,
			Msg:  "invalid org id for permission",
		}
	}

	if p.ID != nil && !(*p.ID).Valid() {
		return &Error{
			Code: EInvalid,
			Err:  ErrInvalidID,
			Msg:  "invalid id for permission",
		}
	}

	return nil
}

// NewPermission returns a permission with provided arguments.
func NewPermission(a Action, r Resource, orgID ID) (*Permission, error) {
	p := &Permission{
		Action:   a,
		Resource: r,
		OrgID:    orgID,
	}

	return p, p.Valid()
}

// NewPermissionAtID creates a permission with the provided arguments.
func NewPermissionAtID(id ID, a Action, r Resource, orgID ID) (*Permission, error) {
	p := &Permission{
		Action:   a,
		Resource: r,
		OrgID:    orgID,
		ID:       &id,
	}

	return p, p.Valid()
}

// OperPermissions are the default permissions for those who setup the application.
func OperPermissions(orgID ID) []Permission {
	ps := []Permission{}
	for _, r := range AllResources {
		for _, a := range actions {
			ps = append(ps, Permission{Action: a, Resource: r, OrgID: orgID})
		}
	}

	return ps
}

// MemberPermissions are the default permissions for those members.
func MemberPermissions(orgID ID) []Permission {
	ps := []Permission{}
	for _, r := range AllResources {
		ps = append(ps, Permission{Action: ReadAction, Resource: r, OrgID: orgID})
	}

	return ps
}
