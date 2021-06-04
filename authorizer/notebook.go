package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.NotebookService = (*NotebookService)(nil)

// NotebookService wraps an influxdb.NotebookService and authorizes actions
// against it appropriately.
type NotebookService struct {
	s influxdb.NotebookService
}

// NewNotebookService constructs an instance of an authorizing check service.
func NewNotebookService(s influxdb.NotebookService) *NotebookService {
	return &NotebookService{
		s: s,
	}
}

// GetNotebook checks to see if the authorizer on context has read access to the id provided.
func (s *NotebookService) GetNotebook(ctx context.Context, id platform.ID) (*influxdb.Notebook, error) {
	nb, err := s.s.GetNotebook(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.NotebooksResourceType, id, nb.OrgID); err != nil {
		return nil, err
	}
	return nb, nil
}

// CreateNotebook checks to see if the authorizer on context has write access for notebooks for organization id provided in the notebook body.
func (s *NotebookService) CreateNotebook(ctx context.Context, create *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	if _, _, err := AuthorizeCreate(ctx, influxdb.NotebooksResourceType, create.OrgID); err != nil {
		return nil, err
	}

	return s.s.CreateNotebook(ctx, create)
}

// UpdateNotebook checks to see if the authorizer on context has write access to the notebook provided.
func (s *NotebookService) UpdateNotebook(ctx context.Context, id platform.ID, update *influxdb.NotebookReqBody) (*influxdb.Notebook, error) {
	nb, err := s.s.GetNotebook(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.NotebooksResourceType, id, nb.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateNotebook(ctx, id, update)
}

// DeleteNotebook checks to see if the authorizer on context has write access to the notebook provided.
func (s *NotebookService) DeleteNotebook(ctx context.Context, id platform.ID) error {
	nb, err := s.s.GetNotebook(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.NotebooksResourceType, id, nb.OrgID); err != nil {
		return err
	}
	return s.s.DeleteNotebook(ctx, id)
}

// ListNotebooks checks to see if the requesting user has read access to the provided org and returns a list of notebooks for that org if so.
func (s *NotebookService) ListNotebooks(ctx context.Context, filter influxdb.NotebookListFilter) ([]*influxdb.Notebook, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.NotebooksResourceType, filter.OrgID); err != nil {
		return nil, err
	}

	return s.s.ListNotebooks(ctx, filter)
}
