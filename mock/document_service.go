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
	TimeGenerator     TimeGenerator
	CreateDocumentFn  func(ctx context.Context, d *influxdb.Document) error
	FindDocumentFn    func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error)
	UpdateDocumentFn  func(ctx context.Context, d *influxdb.Document) error
	DeleteDocumentFn  func(ctx context.Context, id influxdb.ID) error
	FindDocumentsFn   func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error)
	DeleteDocumentsFn func(ctx context.Context, opts ...influxdb.DocumentFindOptions) error
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
		UpdateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
			return nil
		},
		DeleteDocumentFn: func(ctx context.Context, id influxdb.ID) error {
			return nil
		},
		FindDocumentsFn: func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
			return nil, nil
		},
		DeleteDocumentsFn: func(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
			return nil
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

// UpdateDocument will call the mocked UpdateDocumentFn.
func (s *DocumentStore) UpdateDocument(ctx context.Context, d *influxdb.Document) error {
	return s.UpdateDocumentFn(ctx, d)
}

// DeleteDocument will call the mocked DeleteDocumentFn.
func (s *DocumentStore) DeleteDocument(ctx context.Context, id influxdb.ID) error {
	return s.DeleteDocumentFn(ctx, id)
}

// FindDocuments will call the mocked FindDocumentsFn.
func (s *DocumentStore) FindDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
	return s.FindDocumentsFn(ctx, opts...)
}

// DeleteDocuments will call the mocked DeleteDocumentsFn.
func (s *DocumentStore) DeleteDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
	return s.DeleteDocumentsFn(ctx, opts...)
}
