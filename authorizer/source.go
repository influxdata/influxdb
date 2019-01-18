package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
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

func newSourcePermission(a influxdb.Action, orgID, id influxdb.ID) (*influxdb.Permission, error) {
	return influxdb.NewPermissionAtID(id, a, influxdb.SourcesResourceType, orgID)
}

func authorizeReadSource(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newSourcePermission(influxdb.ReadAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

func authorizeWriteSource(ctx context.Context, orgID, id influxdb.ID) error {
	p, err := newSourcePermission(influxdb.WriteAction, orgID, id)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return nil
}

// DefaultSource checks to see if the authorizer on context has read access to the default source.
func (s *SourceService) DefaultSource(ctx context.Context) (*influxdb.Source, error) {
	src, err := s.s.DefaultSource(ctx)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadSource(ctx, src.OrganizationID, src.ID); err != nil {
		return nil, err
	}

	return src, nil
}

// FindSourceByID checks to see if the authorizer on context has read access to the id provided.
func (s *SourceService) FindSourceByID(ctx context.Context, id influxdb.ID) (*influxdb.Source, error) {
	src, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeReadSource(ctx, src.OrganizationID, id); err != nil {
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

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	sources := ss[:0]
	for _, src := range ss {
		err := authorizeReadSource(ctx, src.OrganizationID, src.ID)
		if err != nil && influxdb.ErrorCode(err) != influxdb.EUnauthorized {
			return nil, 0, err
		}

		// TODO(desa): this is a totaly hack and needs to be fixed.
		// Specifically, we need to remove the concept of a default source.
		if src.OrganizationID.String() == bolt.DefaultSourceOrganizationID {
			sources = append(sources, src)
			continue
		}

		if influxdb.ErrorCode(err) == influxdb.EUnauthorized {
			continue
		}

		sources = append(sources, src)
	}

	return sources, len(sources), nil
}

// CreateSource checks to see if the authorizer on context has write access to the global source resource.
func (s *SourceService) CreateSource(ctx context.Context, src *influxdb.Source) error {
	p, err := influxdb.NewPermission(influxdb.WriteAction, influxdb.SourcesResourceType, src.OrganizationID)
	if err != nil {
		return err
	}

	if err := IsAllowed(ctx, *p); err != nil {
		return err
	}

	return s.s.CreateSource(ctx, src)
}

// UpdateSource checks to see if the authorizer on context has write access to the source provided.
func (s *SourceService) UpdateSource(ctx context.Context, id influxdb.ID, upd influxdb.SourceUpdate) (*influxdb.Source, error) {
	src, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if err := authorizeWriteSource(ctx, src.OrganizationID, id); err != nil {
		return nil, err
	}

	return s.s.UpdateSource(ctx, id, upd)
}

// DeleteSource checks to see if the authorizer on context has write access to the source provided.
func (s *SourceService) DeleteSource(ctx context.Context, id influxdb.ID) error {
	m, err := s.s.FindSourceByID(ctx, id)
	if err != nil {
		return err
	}

	if err := authorizeWriteSource(ctx, m.OrganizationID, id); err != nil {
		return err
	}

	return s.s.DeleteSource(ctx, id)
}
