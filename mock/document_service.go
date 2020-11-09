package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ influxdb.DocumentStore = &DocumentStore{}

// DocumentService is mocked document service.
type DocumentService struct {
	CreateDocumentStoreFn func(ctx context.Context, name string) (influxdb.DocumentStore, error)
	FindDocumentStoreFn   func(ctx context.Context, name string) (influxdb.DocumentStore, error)
}

// CreateDocumentStore calls the mocked CreateDocumentStoreFn.
func (s *DocumentService) CreateDocumentStore(ctx context.Context, name string) (influxdb.DocumentStore, error) {
	return s.CreateDocumentStoreFn(ctx, name)
}

// FindDocumentStore calls the mocked FindDocumentStoreFn.
func (s *DocumentService) FindDocumentStore(ctx context.Context, name string) (influxdb.DocumentStore, error) {
	return s.FindDocumentStoreFn(ctx, name)
}

// NewDocumentService returns a mock of DocumentService where its methods will return zero values.
func NewDocumentService() *DocumentService {
	return &DocumentService{
		CreateDocumentStoreFn: func(ctx context.Context, name string) (influxdb.DocumentStore, error) {
			return nil, nil
		},
		FindDocumentStoreFn: func(ctx context.Context, name string) (influxdb.DocumentStore, error) {
			return nil, nil
		},
	}
}

// DocumentStore is the mocked document store.
type DocumentStore struct {
	TimeGenerator    TimeGenerator
	CreateDocumentFn func(ctx context.Context, d *influxdb.Document) error
	FindDocumentFn   func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error)
	FindDocumentsFn  func(ctx context.Context, oid influxdb.ID) ([]*influxdb.Document, error)
}

// NewDocumentStore returns a mock of DocumentStore where its methods will return zero values.
func NewDocumentStore() *DocumentStore {
	return &DocumentStore{
		CreateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
			return nil
		},
		FindDocumentFn: func(ctx context.Context, id influxdb.ID) (document *influxdb.Document, e error) {
			return nil, nil
		},
		FindDocumentsFn: func(ctx context.Context, oid influxdb.ID) ([]*influxdb.Document, error) {
			return nil, nil
		},
	}
}

// CreateDocument will call the mocked CreateDocumentFn.
func (s *DocumentStore) CreateDocument(ctx context.Context, d *influxdb.Document) error {
	return s.CreateDocumentFn(ctx, d)
}

// FindDocument will call the mocked FindDocumentFn.
func (s *DocumentStore) FindDocument(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
	return s.FindDocumentFn(ctx, id)
}

// FindDocuments will call the mocked FindDocumentsFn.
func (s *DocumentStore) FindDocuments(ctx context.Context, oid influxdb.ID) ([]*influxdb.Document, error) {
	return s.FindDocumentsFn(ctx, oid)
}
