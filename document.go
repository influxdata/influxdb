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
	UpdateDocument(ctx context.Context, d *Document) error
	DeleteDocument(ctx context.Context, id ID) error

	FindDocuments(ctx context.Context, opts ...DocumentFindOptions) ([]*Document, error)
	DeleteDocuments(ctx context.Context, opts ...DocumentFindOptions) error
}

// DocumentIndex is a structure that is used in DocumentFindOptions to filter out
// documents based on some criteria.
type DocumentIndex interface {
	GetAccessorsDocuments(ownerType string, ownerID ID) ([]ID, error)
	GetDocumentsAccessors(docID ID) ([]ID, error)

	UsersOrgs(userID ID) ([]ID, error)
	// IsOrgAccessor checks to see if the userID provided is allowed to access
	// the orgID privided. If the lookup is done in a writable operation
	// then this method should ensure that the user is an org owner. If the
	// operation is readable, then it should only require that the user is an org
	// member.
	IsOrgAccessor(userID, orgID ID) error

	// TODO(desa): support finding document by label
	FindOrganizationByName(n string) (ID, error)
	FindOrganizationByID(id ID) error
	FindLabelByID(id ID) error
}

// DocumentDecorator passes information to the DocumentStore about the presentation
// of the data being retrieved.
type DocumentDecorator interface {
	IncludeContent() error
	IncludeLabels() error
	IncludeOrganizations() error
}

// DocumentFindOptions are used to lookup documents.
// TODO(desa): consider changing this to have a single struct that has both
//  the decorator and the index on it.
type DocumentFindOptions func(DocumentIndex, DocumentDecorator) ([]ID, error)

// IncludeContent signals to the DocumentStore that the content of the document
// should be included.
func IncludeContent(_ DocumentIndex, dd DocumentDecorator) ([]ID, error) {
	return nil, dd.IncludeContent()
}

// IncludeLabels signals to the DocumentStore that the documents labels
// should be included.
func IncludeLabels(_ DocumentIndex, dd DocumentDecorator) ([]ID, error) {
	return nil, dd.IncludeLabels()
}

// IncludeOwner signals to the DocumentStore that the owner
// should be included.
func IncludeOrganizations(_ DocumentIndex, dd DocumentDecorator) ([]ID, error) {
	return nil, dd.IncludeOrganizations()
}

// WhereOrg retrieves a list of the ids of the documents that belong to the provided org.
func WhereOrg(org string) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, dec DocumentDecorator) ([]ID, error) {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return nil, err
		}
		return WhereOrgID(oid)(idx, dec)
	}
}

// WhereOrgID retrieves a list of the ids of the documents that belong to the provided orgID.
func WhereOrgID(orgID ID) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		return idx.GetAccessorsDocuments("org", orgID)
	}
}
