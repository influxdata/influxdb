package influxdb

import (
	"context"
	"encoding/json"
)

// consts for checks config.
const (
	CheckDefaultPageSize = 100
	CheckMaxPageSize     = 500
)

// Check represents the information required to generate a periodic check task.
type Check interface {
	Valid(lang FluxLanguageService) error
	Type() string
	ClearPrivateData()
	SetTaskID(ID)
	GetTaskID() ID
	GetOwnerID() ID
	SetOwnerID(ID)
	GenerateFlux(lang FluxLanguageService) (string, error)
	json.Marshaler

	CRUDLogSetter
	SetID(id ID)
	SetOrgID(id ID)
	SetName(name string)
	SetDescription(description string)

	GetID() ID
	GetCRUDLog() CRUDLog
	GetOrgID() ID
	GetName() string
	GetDescription() string
}

// ops for checks error
var (
	OpFindCheckByID = "FindCheckByID"
	OpFindCheck     = "FindCheck"
	OpFindChecks    = "FindChecks"
	OpCreateCheck   = "CreateCheck"
	OpUpdateCheck   = "UpdateCheck"
	OpDeleteCheck   = "DeleteCheck"
)

// CheckService represents a service for managing checks.
type CheckService interface {
	// FindCheckByID returns a single check by ID.
	FindCheckByID(ctx context.Context, id ID) (Check, error)

	// FindCheck returns the first check that matches filter.
	FindCheck(ctx context.Context, filter CheckFilter) (Check, error)

	// FindChecks returns a list of checks that match filter and the total count of matching checks.
	// Additional options provide pagination & sorting.
	FindChecks(ctx context.Context, filter CheckFilter, opt ...FindOptions) ([]Check, int, error)

	// CreateCheck creates a new check and sets b.ID with the new identifier.
	CreateCheck(ctx context.Context, c CheckCreate, userID ID) error

	// UpdateCheck updates the whole check.
	// Returns the new check state after update.
	UpdateCheck(ctx context.Context, id ID, c CheckCreate) (Check, error)

	// PatchCheck updates a single bucket with changeset.
	// Returns the new check state after update.
	PatchCheck(ctx context.Context, id ID, upd CheckUpdate) (Check, error)

	// DeleteCheck will delete the check by id.
	DeleteCheck(ctx context.Context, id ID) error
}

// CheckUpdate are properties than can be updated on a check
type CheckUpdate struct {
	Name        *string `json:"name,omitempty"`
	Status      *Status `json:"status,omitempty"`
	Description *string `json:"description,omitempty"`
}

// CheckCreate represent data to create a new Check
type CheckCreate struct {
	Check
	Status Status `json:"status"`
}

// Valid returns err is the update is invalid.
func (n *CheckUpdate) Valid() error {
	if n.Name != nil && *n.Name == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "Check Name can't be empty",
		}
	}

	if n.Description != nil && *n.Description == "" {
		return &Error{
			Code: EInvalid,
			Msg:  "Check Description can't be empty",
		}
	}

	if n.Status != nil {
		if err := n.Status.Valid(); err != nil {
			return err
		}
	}

	return nil
}

// CheckFilter represents a set of filters that restrict the returned results.
type CheckFilter struct {
	ID    *ID
	Name  *string
	OrgID *ID
	Org   *string
	UserResourceMappingFilter
}

// QueryParams Converts CheckFilter fields to url query params.
func (f CheckFilter) QueryParams() map[string][]string {
	qp := map[string][]string{}

	if f.ID != nil {
		qp["id"] = []string{f.ID.String()}
	}

	if f.Name != nil {
		qp["name"] = []string{*f.Name}
	}

	if f.OrgID != nil {
		qp["orgID"] = []string{f.OrgID.String()}
	}

	if f.Org != nil {
		qp["org"] = []string{*f.Org}
	}

	return qp
}
