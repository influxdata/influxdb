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
