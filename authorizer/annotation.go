package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
)

var _ influxdb.AnnotationService = (*AnnotationService)(nil)

// AnnotationService wraps an influxdb.AnnotationService and authorizes actions
// against it appropriately.
type AnnotationService struct {
	s influxdb.AnnotationService
}

// NewAnnotationService constructs an instance of an authorizing check service.
func NewAnnotationService(s influxdb.AnnotationService) *AnnotationService {
	return &AnnotationService{
		s: s,
	}
}

func (s *AnnotationService) CreateAnnotations(ctx context.Context, orgID platform.ID, create []influxdb.AnnotationCreate) ([]influxdb.AnnotationEvent, error) {
	if _, _, err := AuthorizeCreate(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	return s.s.CreateAnnotations(ctx, orgID, create)
}

func (s *AnnotationService) ListAnnotations(ctx context.Context, orgID platform.ID, filter influxdb.AnnotationListFilter) (influxdb.ReadAnnotations, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	return s.s.ListAnnotations(ctx, orgID, filter)
}

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

func (s *AnnotationService) DeleteAnnotations(ctx context.Context, orgID platform.ID, delete influxdb.AnnotationDeleteFilter) error {
	if _, _, err := AuthorizeOrgWriteResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return err
	}
	return s.s.DeleteAnnotations(ctx, orgID, delete)
}

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

func (s *AnnotationService) ListStreams(ctx context.Context, orgID platform.ID, filter influxdb.StreamListFilter) ([]influxdb.ReadStream, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	return s.s.ListStreams(ctx, orgID, filter)
}

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
	if _, _, err := AuthorizeCreate(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return nil, err
	}

	return s.s.CreateOrUpdateStream(ctx, orgID, stream)
}

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

func (s *AnnotationService) DeleteStreams(ctx context.Context, orgID platform.ID, delete influxdb.BasicStream) error {
	if _, _, err := AuthorizeOrgWriteResource(ctx, influxdb.AnnotationsResourceType, orgID); err != nil {
		return err
	}
	return s.s.DeleteStreams(ctx, orgID, delete)
}

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
