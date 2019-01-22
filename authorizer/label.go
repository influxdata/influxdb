package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

var _ influxdb.LabelService = (*LabelService)(nil)

// LabelService wraps a influxdb.LabelService and authorizes actions
// against it appropriately.
type LabelService struct {
	s influxdb.LabelService
}

// NewLabelService constructs an instance of an authorizing label serivce.
func NewLabelService(s influxdb.LabelService) *LabelService {
	return &LabelService{
		s: s,
	}
}

func newLabelPermission(a influxdb.Action, id influxdb.ID) (*influxdb.Permission, error) {
	p := &influxdb.Permission{
		Action: a,
		Resource: influxdb.Resource{
			Type: influxdb.LabelsResourceType,
			ID:   &id,
		},
	}

	return p, p.Valid()
}

func authorizeReadLabel(ctx context.Context, id influxdb.ID) error {
	p, err := newLabelPermission(influxdb.ReadAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteLabel(ctx context.Context, id influxdb.ID) error {
	p, err := newLabelPermission(influxdb.WriteAction, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// FindLabelByID checks to see if the authorizer on context has read access to the label id provided.
func (s *LabelService) FindLabelByID(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
	if err := authorizeReadLabel(ctx, id); err != nil {
		return nil, err
	}

	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
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

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	labels := ls[:0]
	for _, l := range ls {
		err := authorizeReadLabel(ctx, l.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		labels = append(labels, l)
	}

	return labels, nil
}

func (s *LabelService) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	panic("tbd")
}

// CreateLabel checks to see if the authorizer on context has write access to the global labels resource.
func (s *LabelService) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	p, err := influxdb.NewGlobalPermission(influxdb.WriteAction, influxdb.LabelsResourceType)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateLabel(ctx, l)
}

// CreateLabelMapping checks to see if the authorizer on context has write access to
func (s *LabelService) CreateLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	// note(leodido) > LabelMapping does not have its own ID
	if err := authorizeWriteLabel(ctx, *m.LabelID); err != nil {
		return err
	}

	// todo(leodido) > do we have/want to check also m.ResourceID write permission?
	panic("tbd")
}

// UpdateLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	_, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteLabel(ctx, id); err != nil {
		return nil, err
	}

	return s.s.UpdateLabel(ctx, id, upd)
}

// DeleteLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	_, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteLabel(ctx, id); err != nil {
		return err
	}

	return s.s.DeleteLabel(ctx, id)
}

func (s *LabelService) DeleteLabelMapping(ctx context.Context, m *influxdb.LabelMapping) error {
	panic("tbd")
}
