package authorizer

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.SourceService = (*SourceService)(nil)

// SourceService wraps a influxdb.SourceService and authorizes actions
// against it appropriately.
type SourceService struct {
	s influxdb.SourceService
}

// NewSourceService constructs an instance of an authorizing source service.
func NewSourceService(s influxdb.SourceService) *SourceService {
	return &SourceService{
		s: s,
	}
}

// DefaultSource checks to see if the authorizer on context has read access to the default source.
func (s *SourceService) DefaultSource(ctx context.Context) (*influxdb.Source, error) {
	src, err := s.s.DefaultSource(ctx)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.SourcesResourceType, src.ID, src.OrganizationID); err != nil {
		return nil, err
	}
	return src, nil
}

// FindSourceByID checks to see if the authorizer on context has read access to the id provided.
func (s *SourceService) FindSourceByID(ctx context.Context, id platform.ID) (*influxdb.Source, error) {
	src, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeRead(ctx, influxdb.SourcesResourceType, src.ID, src.OrganizationID); err != nil {
		return nil, err
	}
	return src, nil
}

// FindSources retrieves all sources that match the provided options and then filters the list down to only the resources that are authorized.
func (s *SourceService) FindSources(ctx context.Context, opts influxdb.FindOptions) ([]*influxdb.Source, int, error) {
	// TODO: we'll likely want to push this operation into the database since fetching the whole list of data will likely be expensive.
	ss, _, err := s.s.FindSources(ctx, opts)
	if err != nil {
		return nil, 0, err
	}
	return AuthorizeFindSources(ctx, ss)
}

// CreateSource checks to see if the authorizer on context has write access to the global source resource.
func (s *SourceService) CreateSource(ctx context.Context, src *influxdb.Source) error {
	if _, _, err := AuthorizeCreate(ctx, influxdb.SourcesResourceType, src.OrganizationID); err != nil {
		return err
	}
	return s.s.CreateSource(ctx, src)
}

// UpdateSource checks to see if the authorizer on context has write access to the source provided.
func (s *SourceService) UpdateSource(ctx context.Context, id platform.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
	src, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.SourcesResourceType, src.ID, src.OrganizationID); err != nil {
		return nil, err
	}
	return s.s.UpdateSource(ctx, id, upd)
}

// DeleteSource checks to see if the authorizer on context has write access to the source provided.
func (s *SourceService) DeleteSource(ctx context.Context, id platform.ID) error {
	src, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return err
	}
	if _, _, err := AuthorizeWrite(ctx, influxdb.SourcesResourceType, src.ID, src.OrganizationID); err != nil {
		return err
	}
	return s.s.DeleteSource(ctx, id)
}
