package authorizer

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
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

func (s *documentStore) FindDocuments(ctx context.Context, oid influxdb.ID) ([]*influxdb.Document, error) {
	if _, _, err := AuthorizeOrgReadResource(ctx, influxdb.DocumentsResourceType, oid); err != nil {
		return nil, err
	}

	return s.s.FindDocuments(ctx, oid)
}
