package mock

import (
	"context"

	"github.com/influxdata/influxdb"
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
	CreateDocumentFn  func(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error
	UpdateDocumentFn  func(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error
	FindDocumentsFn   func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error)
	DeleteDocumentsFn func(ctx context.Context, opts ...influxdb.DocumentFindOptions) error
}

// NewDocumentStore returns a mock of DocumentStore where its methods will return zero values.
func NewDocumentStore() *DocumentStore {
	return &DocumentStore{
		CreateDocumentFn: func(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
			return nil
		},
		UpdateDocumentFn: func(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
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
func (s *DocumentStore) CreateDocument(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
	return s.CreateDocumentFn(ctx, d, opts...)
}

// UpdateDocument will call the mocked UpdateDocumentFn.
func (s *DocumentStore) UpdateDocument(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
	return s.UpdateDocumentFn(ctx, d, opts...)
}

// FindDocuments will call the mocked FindDocumentsFn.
func (s *DocumentStore) FindDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
	return s.FindDocumentsFn(ctx, opts...)
}

// DeleteDocuments will call the mocked DeleteDocumentsFn.
func (s *DocumentStore) DeleteDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
	return s.DeleteDocumentsFn(ctx, opts...)
}
