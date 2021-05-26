package influxdb

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	ErrOrgIDRequired  = fieldRequiredError("OrgID")
	ErrNameRequired   = fieldRequiredError("Name")
	ErrSpecRequired   = fieldRequiredError("Spec")
	ErrOffsetNegative = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "offset cannot be negative",
	}
	ErrLimitLTEZero = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "limit cannot be less-than or equal-to zero",
	}
	ErrNotebookNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "notebook not found",
	}
)

func fieldRequiredError(field string) error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("%s required", field),
	}
}

// Notebook represents all visual and query data for a notebook.
type Notebook struct {
	OrgID     platform.ID  `json:"orgID" db:"org_id"`
	ID        platform.ID  `json:"id" db:"id"`
	Name      string       `json:"name" db:"name"`
	Spec      NotebookSpec `json:"spec" db:"spec"`
	CreatedAt time.Time    `json:"createdAt" db:"created_at"`
	UpdatedAt time.Time    `json:"updatedAt" db:"updated_at"`
}

// NotebookSpec is an abitrary JSON object provided by the client.
type NotebookSpec map[string]interface{}

// Value implements the database/sql Valuer interface for adding NotebookSpecs to the database.
func (s NotebookSpec) Value() (driver.Value, error) {
	spec, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return string(spec), nil
}

// Scan implements the database/sql Scanner interface for retrieving NotebookSpecs from the database.
func (s *NotebookSpec) Scan(value interface{}) error {
	var spec NotebookSpec
	if err := json.NewDecoder(strings.NewReader(value.(string))).Decode(&spec); err != nil {
		return err
	}

	*s = spec
	return nil
}

// NotebookService is the service contract for Notebooks.
type NotebookService interface {
	GetNotebook(ctx context.Context, id platform.ID) (*Notebook, error)
	CreateNotebook(ctx context.Context, create *NotebookReqBody) (*Notebook, error)
	UpdateNotebook(ctx context.Context, id platform.ID, update *NotebookReqBody) (*Notebook, error)
	DeleteNotebook(ctx context.Context, id platform.ID) error
	ListNotebooks(ctx context.Context, filter NotebookListFilter) ([]*Notebook, error)
}

// NotebookListFilter is a selection filter for listing notebooks.
type NotebookListFilter struct {
	OrgID platform.ID
	Page  Page
}

// Page contains pagination information
type Page struct {
	Offset int
	Limit  int
}

// Validate validates the Page
func (p Page) Validate() error {
	if p.Offset < 0 {
		return ErrOffsetNegative
	}
	if p.Limit <= 0 {
		return ErrLimitLTEZero
	}
	return nil
}

// NotebookReqBody contains fields for creating or updating notebooks.
type NotebookReqBody struct {
	OrgID platform.ID  `json:"orgID"`
	Name  string       `json:"name"`
	Spec  NotebookSpec `json:"spec"`
}

// Validate validates the creation object
func (n NotebookReqBody) Validate() error {
	if !n.OrgID.Valid() {
		return ErrOrgIDRequired
	}
	if n.Name == "" {
		return ErrNameRequired
	}
	if n.Spec == nil {
		return ErrSpecRequired
	}
	return nil
}
