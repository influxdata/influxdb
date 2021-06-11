package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var _ influxdb.AnnotationService = (*AnnotationService)(nil)

// AnnotationService wraps an influxdb.AnnotationService and authorizes actions
// against it appropriately.
type AnnotationService struct {
	s influxdb.AnnotationService
}

// NewAnnotationService constructs an instance of an authorizing check service
func NewAnnotationService(s influxdb.AnnotationService) *AnnotationService {
	return &AnnotationService{
		s: s,
	}
}

// CreateAnnotations checks to see if the authorizer on context has write access for annotations for the provided orgID
func (s *AnnotationService) CreateAnnotations(ctx context.Context, orgID platform.ID, create []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	if _, _, err := AuthorizeCreate(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	return s.s.CreateAnnotations(ctx, orgID, create)
}

// ListAnnotations checks to see if the authorizer on context has read access for annotations for the provided orgID
// and then filters the list down to only the resources that are authorized
func (s *AnnotationService) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) ([]influxdb.StoredAnnotation, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	as, err := s.s.ListAnnotations(ctx, orgID, filter)
	if err != nil {
		return nil, err
	}

	as, _, err = AuthorizeFindAnnotations(ctx, as)
	return as, err
}

// GetAnnotation checks to see if the authorizer on context has read access to the requested annotation
func (s *AnnotationService) GetAnnotation(ctx context.Context, id platform.ID) (*influxdb.StoredAnnotation, error) {
	a, err := s.s.GetAnnotation(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.AnnotationsResourceType, id, a.OrgID); err != nil {
		return nil, err
	}
	return a, nil
}

// DeleteAnnotations checks to see if the authorizer on context has write access to the provided orgID
func (s *AnnotationService) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) error {
	if _, _, err := AuthorizeOrgWriteResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return err
	}
	return s.s.DeleteAnnotations(ctx, orgID, delete)
}

// DeleteAnnotation checks to see if the authorizer on context has write access to the requested annotation
func (s *AnnotationService) DeleteAnnotation(ctx context.Context, id platform.ID) error {
	a, err := s.s.GetAnnotation(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AnnotationsResourceType, id, a.OrgID); err != nil {
		return err
	}
	return s.s.DeleteAnnotation(ctx, id)
}

// UpdateAnnotation checks to see if the authorizer on context has write access to the requested annotation
func (s *AnnotationService) UpdateAnnotation(ctx context.Context, id platform.ID, update influxdb.AnnotationCreate) (*influxdb.AnnotationEvent, error) {
	a, err := s.s.GetAnnotation(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AnnotationsResourceType, id, a.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateAnnotation(ctx, id, update)
}

// ListStreams checks to see if the authorizer on context has read access for streams for the provided orgID
// and then filters the list down to only the resources that are authorized
func (s *AnnotationService) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) ([]influxdb.StoredStream, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	ss, err := s.s.ListStreams(ctx, orgID, filter)
	if err != nil {
		return nil, err
	}

	ss, _, err = AuthorizeFindStreams(ctx, ss)
	return ss, err
}

// GetStream checks to see if the authorizer on context has read access to the requested stream
func (s *AnnotationService) GetStream(ctx context.Context, id platform.ID) (*influxdb.StoredStream, error) {
	st, err := s.s.GetStream(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.AnnotationsResourceType, id, st.OrgID); err != nil {
		return nil, err
	}
	return st, nil
}

func (s *AnnotationService) CreateOrUpdateStream(ctx context.Context, orgID platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	// We need to know if the request is creating a new stream, or updating an existing stream to check
	// permissions appropriately

	// Get the stream by name. An empty slice will be returned if the stream doesn't exist
	// note: a given org can only have one stream by the same name. this constraint is enforced in the database schema
	streams, err := s.s.ListStreams(ctx, orgID, influxdb.StreamListFilter{
		StreamIncludes: []string{stream.Name},
	})
	if err != nil {
		return nil, err
	}

	// update an already existing stream
	if len(streams) == 1 {
		return s.UpdateStream(ctx, streams[0].ID, stream)
	}

	// create a new stream if one doesn't already exist
	if len(streams) == 0 {
		if _, _, err := AuthorizeCreate(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
			return nil, err
		}

		return s.s.CreateOrUpdateStream(ctx, orgID, stream)
	}

	// if multiple streams were returned somehow, return an error
	// this should never happen, so return a server error
	return nil, &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("more than one stream named %q for org %q", streams[0].Name, orgID),
	}
}

// UpdateStream checks to see if the authorizer on context has write access to the requested stream
func (s *AnnotationService) UpdateStream(ctx context.Context, id platform.ID, stream influxdb.Stream) (*influxdb.ReadStream, error) {
	st, err := s.s.GetStream(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AnnotationsResourceType, id, st.OrgID); err != nil {
		return nil, err
	}
	return s.s.UpdateStream(ctx, id, stream)
}

// DeleteStreams checks to see if the authorizer on context has write access to the provided orgID
func (s *AnnotationService) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) error {
	if _, _, err := AuthorizeOrgWriteResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return err
	}
	return s.s.DeleteStreams(ctx, orgID, delete)
}

// DeleteStreamByID checks to see if the authorizer on context has write access to the requested stream
func (s *AnnotationService) DeleteStreamByID(ctx context.Context, id platform.ID) error {
	st, err := s.s.GetStream(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.AnnotationsResourceType, id, st.OrgID); err != nil {
		return err
	}
	return s.s.DeleteStreamByID(ctx, id)
}
