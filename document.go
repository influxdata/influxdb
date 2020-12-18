package influxdb

import (
	"context"
)

// ErrDocumentNotFound is the error msg for a missing document.
const ErrDocumentNotFound = "document not found"

// DocumentService is used to create/find instances of document stores.
type DocumentService interface {
	CreateDocumentStore(ctx context.Context, name string) (DocumentStore, error)
	FindDocumentStore(ctx context.Context, name string) (DocumentStore, error)
}

// Document is a generic structure for stating data.
type Document struct {
	ID      ID           `json:"id"`
	Meta    DocumentMeta `json:"meta"`
	Content interface{}  `json:"content,omitempty"` // TODO(desa): maybe this needs to be json.Marshaller & json.Unmarshaler
	Labels  []*Label     `json:"labels,omitempty"`  // read only

	// This is needed for authorization.
	// The service that passes documents around will take care of filling it
	// via request parameters or others, as the kv store will take care of
	// filling it once it returns a document.
	// This is not stored in the kv store neither required in the API.
	Organizations map[ID]UserType `json:"-"`
}

// DocumentMeta is information that is universal across documents. Ideally
// data in the meta should be indexed and queryable.
type DocumentMeta struct {
	Name        string `json:"name"`
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version,omitempty"`
	CRUDLog
}

// DocumentStore is used to perform CRUD operations on documents. It follows an options
// pattern that allows users to perform actions related to documents in a transactional way.
type DocumentStore interface {
	CreateDocument(ctx context.Context, d *Document) error
	FindDocument(ctx context.Context, id ID) (*Document, error)

	FindDocuments(ctx context.Context, orgID ID) ([]*Document, error)
}
