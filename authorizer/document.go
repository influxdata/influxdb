package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

var _ influxdb.DocumentService = (*DocumentService)(nil)
var _ influxdb.DocumentStore = (*documentStore)(nil)

type DocumentService struct {
	s influxdb.DocumentService
}

// NewDocumentService constructs an instance of an authorizing document service.
func NewDocumentService(s influxdb.DocumentService) influxdb.DocumentService {
	return &DocumentService{
		s: s,
	}
}

func (s *DocumentService) CreateDocumentStore(ctx context.Context, name string) (influxdb.DocumentStore, error) {
	ds, err := s.s.CreateDocumentStore(ctx, name)
	if err != nil {
		return nil, err
	}
	return &documentStore{s: ds}, nil
}

func (s *DocumentService) FindDocumentStore(ctx context.Context, name string) (influxdb.DocumentStore, error) {
	ds, err := s.s.FindDocumentStore(ctx, name)
	if err != nil {
		return nil, err
	}
	return &documentStore{s: ds}, nil
}

type documentStore struct {
	s influxdb.DocumentStore
}

func newDocumentPermission(a influxdb.Action, orgID influxdb.ID, did *influxdb.ID) (*influxdb.Permission, error) {
	if did != nil {
		return influxdb.NewPermissionAtID(*did, a, influxdb.DocumentsResourceType, orgID)
	}
	return influxdb.NewPermission(a, influxdb.DocumentsResourceType, orgID)
}

func toPerms(action influxdb.Action, orgs map[influxdb.ID]influxdb.UserType, did *influxdb.ID) ([]influxdb.Permission, error) {
	ps := make([]influxdb.Permission, 0, len(orgs))
	for orgID := range orgs {
		p, err := newDocumentPermission(action, orgID, did)
		if err != nil {
			return nil, err
		}
		ps = append(ps, *p)
	}
	return ps, nil
}

func (s *documentStore) CreateDocument(ctx context.Context, d *influxdb.Document) error {
	if len(d.Organizations) == 0 {
		return fmt.Errorf("cannot authorize document creation without any orgID")
	}
	ps, err := toPerms(influxdb.WriteAction, d.Organizations, nil)
	if err != nil {
		return err
	}
	if err := IsAllowedAny(ctx, ps); err != nil {
		return err
	}
	return s.s.CreateDocument(ctx, d)
}

func (s *documentStore) FindDocument(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
	d, err := s.s.FindDocument(ctx, id)
	if err != nil {
		return nil, err
	}
	ps, err := toPerms(influxdb.ReadAction, d.Organizations, &id)
	if err != nil {
		return nil, err
	}
	if err := IsAllowedAny(ctx, ps); err != nil {
		return nil, err
	}
	return d, nil
}

func (s *documentStore) UpdateDocument(ctx context.Context, d *influxdb.Document) error {
	if len(d.Organizations) == 0 {
		// Cannot authorize without orgs
		ds, err := s.s.FindDocument(ctx, d.ID)
		if err != nil {
			return err
		}
		d.Organizations = ds.Organizations
	}
	ps, err := toPerms(influxdb.WriteAction, d.Organizations, &d.ID)
	if err != nil {
		return err
	}
	if err := IsAllowedAny(ctx, ps); err != nil {
		return err
	}
	return s.s.UpdateDocument(ctx, d)
}

func (s *documentStore) DeleteDocument(ctx context.Context, id influxdb.ID) error {
	d, err := s.s.FindDocument(ctx, id)
	if err != nil {
		return err
	}
	ps, err := toPerms(influxdb.WriteAction, d.Organizations, &id)
	if err != nil {
		return err
	}
	if err := IsAllowedAny(ctx, ps); err != nil {
		return err
	}
	return s.s.DeleteDocument(ctx, id)
}

func (s *documentStore) findDocs(ctx context.Context, action influxdb.Action, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
	// TODO: we'll likely want to push this operation into the database eventually since fetching the whole list of data
	//  will likely be expensive.
	opts = append(opts, influxdb.IncludeOrganizations)
	ds, err := s.s.FindDocuments(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// This filters without allocating
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	fds := ds[:0]
	for _, d := range ds {
		ps, err := toPerms(action, d.Organizations, &d.ID)
		if err != nil {
			return nil, err
		}
		if err := IsAllowedAny(ctx, ps); err != nil {
			// If you are looking for docs for reading, then use permissions as a filter.
			// If you are editing, then, you are not allowed.
			if action == influxdb.ReadAction {
				continue
			} else {
				return nil, err
			}
		}
		fds = append(fds, d)
	}
	return fds, nil
}

func (s *documentStore) FindDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
	return s.findDocs(ctx, influxdb.ReadAction, opts...)
}

func (s *documentStore) DeleteDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
	ds, err := s.findDocs(ctx, influxdb.WriteAction, opts...)
	if err != nil {
		return err
	}
	ids := make([]influxdb.ID, len(ds))
	for i, d := range ds {
		ids[i] = d.ID
	}
	return s.s.DeleteDocuments(ctx,
		func(_ influxdb.DocumentIndex, _ influxdb.DocumentDecorator) (ids []influxdb.ID, e error) {
			return ids, nil
		},
	)
}
