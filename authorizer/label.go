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

func newLabelPermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.LabelsResourceType, orgID)
}

func newResourcePermission(a influxdb.Action, id influxdb.ID, resourceType influxdb.ResourceType) (*influxdb.Permission, error) {
	if err := resourceType.Valid(); err != nil {
		return nil, err
	}

	p := &influxdb.Permission{
		Action: a,
		Resource: influxdb.Resource{
			Type: resourceType,
			ID:   &id,
		},
	}

	return p, p.Valid()
}

func authorizeLabelMappingAction(ctx context.Context, action influxdb.Action, id influxdb.ID, resourceType influxdb.ResourceType) error {
	p, err := newResourcePermission(action, id, resourceType)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// NOTE(affo): documents only create a URM of type `OrgMappingType`.
// So, the permissions are not user-scoped, but org-scoped.
// When a user authenticates he/she is allowed to read/write documents for an org, instead,
// in other services, a user is allowed to read/write specific resources (e.g. dashboard with ID xxx).
// This makes documents a special case for the label service.
// Changing labels for a document must be checked against a permission for the org the user is in,
// not for the specific document.
// However we don't know the orgs for the user, so, the best we can do, is to check that the user has
// permissions for the label's org rather than the document's.
func authorizeDocumentLabelMappingAction(ctx context.Context, action influxdb.Action, orgID influxdb.ID) error {
	p, err := newDocumentOrgPermission(action, orgID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeReadLabel(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newLabelPermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteLabel(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newLabelPermission(influxdb.WriteAction, orgID, id)
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
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadLabel(ctx, l.OrgID, id); err != nil {
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
		err := authorizeReadLabel(ctx, l.OrgID, l.ID)
		if err != nil &&
			influxdb.ErrorCode(err) != influxdb.EUnauthorized &&
			influxdb.ErrorCode(err) != influxdb.EInvalid {
			return nil, err
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized ||
			influxdb.ErrorCode(err) == influxdb.EInvalid {
			continue
		}

		labels = append(labels, l)
	}

	return labels, nil
}

// FindResourceLabels retrieves all labels belonging to the filtering resource if the authorizer on context has read access to it.
// Then it filters the list down to only the labels that are authorized.
func (s *LabelService) FindResourceLabels(ctx context.Context, filter influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
	// NOTE(affo): see `authorizeDocumentLabelMappingAction` note.
	//  The best we can do here is to skip this first check because we don't have
	//  any document-specific permission available.
	//  Then, we canm check that the user is authorized to access documents under the label's orgID.
	if filter.ResourceType != influxdb.DocumentsResourceType {
		if err := authorizeLabelMappingAction(ctx, influxdb.ReadAction, filter.ResourceID, filter.ResourceType); err != nil {
			return nil, err
		}
	}

	ls, err := s.s.FindResourceLabels(ctx, filter)
	if err != nil {
		return nil, err
	}

	labels := ls[:0]
	for _, l := range ls {
		var err error
		err = authorizeDocumentLabelMappingAction(ctx, influxdb.ReadAction, l.OrgID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, err
		}
		err = authorizeReadLabel(ctx, l.OrgID, l.ID)
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

// CreateLabel checks to see if the authorizer on context has read access to the new label's org.
func (s *LabelService) CreateLabel(ctx context.Context, l *influxdb.Label) error {
	if err := authorizeReadOrg(ctx, l.OrgID); err != nil {
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

	if err := authorizeWriteLabel(ctx, l.OrgID, m.LabelID); err != nil {
		return err
	}

	// NOTE(affo): see `authorizeDocumentLabelMappingAction` note.
	//  The best we can do here is to check that the user is authorized to access documents
	//  under the label's orgID, because we don't have any document-specific permission available.
	if m.ResourceType == influxdb.DocumentsResourceType {
		if err := authorizeDocumentLabelMappingAction(ctx, influxdb.ReadAction, l.OrgID); err != nil {
			return err
		}
	} else {
		if err := authorizeLabelMappingAction(ctx, influxdb.WriteAction, m.ResourceID, m.ResourceType); err != nil {
			return err
		}
	}

	return s.s.CreateLabelMapping(ctx, m)
}

// UpdateLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) UpdateLabel(ctx context.Context, id influxdb.ID, upd influxdb.LabelUpdate) (*influxdb.Label, error) {
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteLabel(ctx, l.OrgID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateLabel(ctx, id, upd)
}

// DeleteLabel checks to see if the authorizer on context has write access to the label provided.
func (s *LabelService) DeleteLabel(ctx context.Context, id influxdb.ID) error {
	l, err := s.s.FindLabelByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteLabel(ctx, l.OrgID, id); err != nil {
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

	if err := authorizeWriteLabel(ctx, l.OrgID, m.LabelID); err != nil {
		return err
	}

	// NOTE(affo): see `authorizeDocumentLabelMappingAction` note.
	//  The best we can do here is to check that the user is authorized to access documents
	//  under the label's orgID, because we don't have any document-specific permission available.
	if m.ResourceType == influxdb.DocumentsResourceType {
		if err := authorizeDocumentLabelMappingAction(ctx, influxdb.ReadAction, l.OrgID); err != nil {
			return err
		}
	} else {
		if err := authorizeLabelMappingAction(ctx, influxdb.WriteAction, m.ResourceID, m.ResourceType); err != nil {
			return err
		}
	}

	return s.s.DeleteLabelMapping(ctx, m)
}
