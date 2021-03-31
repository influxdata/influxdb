package influxdb

import (
	"context"
	"encoding/json"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
)

// consts for checks config.
const (
	CheckDefaultPageSize = 100
	CheckMaxPageSize     = 500
)

// Check represents the information required to generate a periodic check task.
type Check interface {
	Valid(lang fluxlang.FluxLanguageService) error
	Type() string
	ClearPrivateData()
	SetTaskID(platform.ID)
	GetTaskID() platform.ID
	GetOwnerID() platform.ID
	SetOwnerID(platform.ID)
	GenerateFlux(lang fluxlang.FluxLanguageService) (string, error)
	json.Marshaler

	CRUDLogSetter
	SetID(id platform.ID)
	SetOrgID(id platform.ID)
	SetName(name string)
	SetDescription(description string)

	GetID() platform.ID
	GetCRUDLog() CRUDLog
	GetOrgID() platform.ID
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
	FindCheckByID(ctx context.Context, id platform.ID) (Check, error)

	// FindCheck returns the first check that matches filter.
	FindCheck(ctx context.Context, filter CheckFilter) (Check, error)

	// FindChecks returns a list of checks that match filter and the total count of matching checks.
	// Additional options provide pagination & sorting.
	FindChecks(ctx context.Context, filter CheckFilter, opt ...FindOptions) ([]Check, int, error)

	// CreateCheck creates a new check and sets b.ID with the new identifier.
	CreateCheck(ctx context.Context, c CheckCreate, userID platform.ID) error

	// UpdateCheck updates the whole check.
	// Returns the new check state after update.
	UpdateCheck(ctx context.Context, id platform.ID, c CheckCreate) (Check, error)

	// PatchCheck updates a single bucket with changeset.
	// Returns the new check state after update.
	PatchCheck(ctx context.Context, id platform.ID, upd CheckUpdate) (Check, error)

	// DeleteCheck will delete the check by id.
	DeleteCheck(ctx context.Context, id platform.ID) error
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
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Check Name can't be empty",
		}
	}

	if n.Description != nil && *n.Description == "" {
		return &errors.Error{
			Code: errors.EInvalid,
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
	ID    *platform.ID
	Name  *string
	OrgID *platform.ID
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
