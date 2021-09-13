package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.LabelService = (*LabelService)(nil)

// LabelService wraps a influxdb.LabelService and authorizes actions
// against it appropriately.
type LabelService struct {
	s             influxdb.LabelService
	orgIDResolver OrgIDResolver
}

// NewLabelServiceWithOrg constructs an instance of an authorizing label service.
// Replaces NewLabelService.
func NewLabelServiceWithOrg(s influxdb.LabelService, orgIDResolver OrgIDResolver) *LabelService {
	return &LabelService{
		s:             s,
		orgIDResolver: orgIDResolver,
	}
}

// FindLabelByID checks to see if the authorizer on context has read access to the label id provided.
func (s *LabelService) FindLabelByID(ctx context.Context, id platform.ID) (*influxdb.Label, error) {
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.LabelsResourceType, id, l.OrgID); err != nil {
		return nil, err
	}
	return l, nil
}

// FindLabels retrieves all labels that match the provided filter and then filters the list down to only the resources that are authorized.
func (s *LabelService) FindLabels(ctx context.Context, filter influxdb.LabelFilter, opt ...influxdb.FindOptions) ([]*influxdb.Label, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	// will likely be expensive.
	ls, err := s.s.FindLabels(ctx, filter, opt...)
	if err != nil {
		return nil, err
	}
	ls, _, err = AuthorizeFindLabels(ctx, ls)
	return ls, err
}

// FindResourceLabels retrieves all labels belonging to the filtering resource if the authorizer on context has read access to it.
// Then it filters the list down to only the labels that are authorized.
func (s *LabelService) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	if err := filter.ResourceType.Valid(); err != nil {
		return nil, err
	}

	orgID, err := s.orgIDResolver.FindResourceOrganizationID(ctx, filter.ResourceType, filter.ResourceID)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, filter.ResourceType, filter.ResourceID, orgID); err != nil {
		return nil, err
	}

	ls, err := s.s.FindResourceLabels(ctx, filter)
	if err != nil {
		return nil, err
	}
	ls, _, err = AuthorizeFindLabels(ctx, ls)
	return ls, err
}

// CreateLabel checks to see if the authorizer on context has write access to the new label's org.
func (s *LabelService) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.LabelsResourceType, l.OrgID); err != nil {
		return err
	}
	return s.s.CreateLabel(ctx, l)
}

// CreateLabelMapping checks to see if the authorizer on context has write access to the label and the resource contained by the label mapping in creation.
func (s *LabelService) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	l, err := s.s.FindLabelByID(ctx, m.LabelID)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.LabelsResourceType, m.LabelID, l.OrgID); err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, m.ResourceType, m.ResourceID, l.OrgID); err != nil {
		return err
	}
	return s.s.CreateLabelMapping(ctx, m)
}

// UpdateLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) UpdateLabel(ctx context.Context, id platform.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.LabelsResourceType, l.ID, l.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateLabel(ctx, id, upd)
}

// DeleteLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) DeleteLabel(ctx context.Context, id platform.ID) error {
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.LabelsResourceType, l.ID, l.OrgID); err != nil {
		return err
	}
	return s.s.DeleteLabel(ctx, id)
}

// DeleteLabelMapping checks to see if the authorizer on context has write access to the label and the resource of the label mapping to delete.
func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	l, err := s.s.FindLabelByID(ctx, m.LabelID)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.LabelsResourceType, m.LabelID, l.OrgID); err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, m.ResourceType, m.ResourceID, l.OrgID); err != nil {
		return err
	}
	return s.s.DeleteLabelMapping(ctx, m)
}
