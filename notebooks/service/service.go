package service

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// Notebook represents all visual and query data for a notebook.
type Notebook struct {
	OrgID     platform.ID  `json:"orgID"`
	ID        platform.ID  `json:"id"`
	Name      string       `json:"name"`
	Spec      NotebookSpec `json:"spec"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

// NotebookSpec is an abitrary JSON object provided by the client.
type NotebookSpec map[string]interface{}

// NotebookService is the service contract for Notebooks.
type NotebookService interface {
	GetNotebook(ctx context.Context, orgID platform.ID, id platform.ID) (*Notebook, error)
	CreateNotebook(ctx context.Context, create NotebookCreate) (*Notebook, error)
	UpdateNotebook(ctx context.Context, orgID platform.ID, id platform.ID, update NotebookUpdate) (*Notebook, error)
	DeleteNotebook(ctx context.Context, orgID platform.ID, id platform.ID) error
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

// NotebookCreate contains fields for creating notebooks.
type NotebookCreate struct {
	OrgID platform.ID
	Name  string
	Spec  NotebookSpec
}

// Validate validates the creation object
func (n NotebookCreate) Validate() error {
	if !n.OrgID.Valid() {
		return ErrOrgIDRequired
	}
	if n.Name == "" {
		return ErrNameRequired
	}
	return nil
}

// NotebookUpdate represents an update request.
type NotebookUpdate struct {
	Name string
	Spec NotebookSpec
}

// Validate validates the update object
func (n NotebookUpdate) Validate() error {
	if n.Name == "" {
		return ErrNameRequired
	}
	if n.Spec == nil {
		return ErrSpecRequired
	}
	return nil
}
