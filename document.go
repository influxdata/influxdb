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
}

// DocumentMeta is information that is universal across documents. Ideally
// data in the meta should be indexed and queryable.
type DocumentMeta struct {
	Name        string `json:"name"`
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	Version     string `json:"version,omitempty"`
}

// DocumentStore is used to perform CRUD operations on documents. It follows an options
// pattern that allows users to perform actions related to documents in a transactional way.
type DocumentStore interface {
	CreateDocument(ctx context.Context, d *Document, opts ...DocumentOptions) error
	UpdateDocument(ctx context.Context, d *Document, opts ...DocumentOptions) error

	FindDocuments(ctx context.Context, opts ...DocumentFindOptions) ([]*Document, error)
	DeleteDocuments(ctx context.Context, opts ...DocumentFindOptions) error
}

// DocumentIndex is a structure that is used in DocumentOptions to perform operations
// related to labels and ownership.
type DocumentIndex interface {
	// TODO(desa): support users as document owners eventually
	AddDocumentOwner(docID ID, ownerType string, ownerID ID) error
	RemoveDocumentOwner(docID ID, ownerType string, ownerID ID) error

	GetAccessorsDocuments(ownerType string, ownerID ID) ([]ID, error)
	GetDocumentsAccessors(docID ID) ([]ID, error)

	UsersOrgs(userID ID) ([]ID, error)
	// IsOrgAccessor checks to see if the userID provided is allowed to access
	// the orgID privided. If the lookup is done in a writable operation
	// then this method should ensure that the user is an org owner. If the
	// operation is readable, then it should only require that the user is an org
	// member.
	IsOrgAccessor(userID, orgID ID) error

	FindOrganizationByName(n string) (ID, error)
	FindOrganizationByID(id ID) error
	FindLabelByID(id ID) error

	AddDocumentLabel(docID, labelID ID) error
	RemoveDocumentLabel(docID, labelID ID) error

	// TODO(desa): support finding document by label
}

// DocumentDecorator passes information to the DocumentStore about the presentation
// of the data being retrieved. It can be used to include the content or the labels
// associated with a document.
type DocumentDecorator interface {
	IncludeContent() error
	IncludeLabels() error
	// TODO(desa): add support for including owners.
}

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

// DocumentOptions are specified during create/update. They can be used to add labels/owners
// to documents. During Create, options are executed after the creation of the document has
// taken place. During Update, they happen before.
type DocumentOptions func(ID, DocumentIndex) error

// DocumentFindOptions are speficied during find/delete. They are used to lookup
// documents using labels/owners.
// TODO(desa): consider changing this to have a single struct that has both
// the decorator and the index on it.
type DocumentFindOptions func(DocumentIndex, DocumentDecorator) ([]ID, error)

// WithOrg adds the provided org as an owner of the document.
func WithOrg(org string) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return err
		}

		return idx.AddDocumentOwner(id, "org", oid)
	}
}

// AuthorizedWithOrg adds the provided org as an owner of the document if
// the authorizer is allowed to access the org in being added.
func AuthorizedWithOrg(a Authorizer, org string) func(ID, DocumentIndex) error {
	switch t := a.(type) {
	case *Authorization:
		return TokenAuthorizedWithOrg(t, org)
	}

	return func(id ID, idx DocumentIndex) error {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return err
		}

		if err := idx.IsOrgAccessor(a.GetUserID(), oid); err != nil {
			return err
		}

		return idx.AddDocumentOwner(id, "org", oid)
	}
}

// TokenAuthorizedWithOrg ensures that the authorization provided is allowed to perform
// write actions against the org provided.
func TokenAuthorizedWithOrg(a *Authorization, org string) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		if !a.IsActive() {
			return &Error{
				Code: EUnauthorized,
				Msg:  "authorization cannot add org as document owner",
			}
		}

		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return err
		}

		p := Permission{
			Action: WriteAction,
			Resource: Resource{
				Type:  DocumentsResourceType,
				OrgID: &oid,
			},
		}

		if !a.Allowed(p) {
			return &Error{
				Code: EUnauthorized,
				Msg:  "authorization cannot add org as document owner",
			}
		}

		return idx.AddDocumentOwner(id, "org", oid)
	}
}

// WithOrgID adds the provided org as an owner of the document.
func WithOrgID(orgID ID) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		return idx.AddDocumentOwner(id, "org", orgID)
	}
}

// AuthorizedWithOrgID adds the provided org as an owner of the document if
// the authorizer is allowed to access the org in being added.
func AuthorizedWithOrgID(a Authorizer, orgID ID) func(ID, DocumentIndex) error {
	switch t := a.(type) {
	case *Authorization:
		return TokenAuthorizedWithOrgID(t, orgID)
	}

	return func(id ID, idx DocumentIndex) error {
		if err := idx.IsOrgAccessor(a.GetUserID(), orgID); err != nil {
			return err
		}

		return idx.AddDocumentOwner(id, "org", orgID)
	}
}

// TokenAuthorizedWithOrgID ensures that the authorization provided is allowed to perform
// write actions against the org provided.
func TokenAuthorizedWithOrgID(a *Authorization, orgID ID) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		if !a.IsActive() {
			return &Error{
				Code: EUnauthorized,
				Msg:  "authorization cannot add org as document owner",
			}
		}

		p := Permission{
			Action: WriteAction,
			Resource: Resource{
				Type:  DocumentsResourceType,
				OrgID: &orgID,
			},
		}

		if !a.Allowed(p) {
			return &Error{
				Code: EUnauthorized,
				Msg:  "authorization cannot add org as document owner",
			}
		}

		return idx.AddDocumentOwner(id, "org", orgID)
	}
}

// WithLabel adds a label to the documents where it is applied.
func WithLabel(lid ID) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		// TODO(desa): turns out that labels are application global, at somepoint we'll
		// want to scope these by org. We should add auth at that point.
		err := idx.FindLabelByID(lid)
		if err != nil {
			return err
		}

		return idx.AddDocumentLabel(id, lid)
	}
}

// WithoutLabel removes a label to the documents where it is applied.
func WithoutLabel(lid ID) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		// TODO(desa): turns out that labels are application global, at somepoint we'll
		// want to scope these by org. We should add auth at that point.
		err := idx.FindLabelByID(lid)
		if err != nil {
			return err
		}

		return idx.RemoveDocumentLabel(id, lid)
	}
}

// Authorized checks to see if the user is authorized to access the document provided.
// If the authorizer is a token, then it checks the tokens permissions. Otherwise,
// it checks to see if the user associated with the authorizer is an accessor
// of the of the org that owns the document.
func Authorized(a Authorizer) func(ID, DocumentIndex) error {
	switch t := a.(type) {
	case *Authorization:
		return TokenAuthorized(t)
	}

	return func(docID ID, idx DocumentIndex) error {
		oids, err := idx.GetDocumentsAccessors(docID)
		if err != nil {
			return err
		}

		for _, oid := range oids {
			if err := idx.IsOrgAccessor(a.GetUserID(), oid); err == nil {
				return nil
			}
		}

		return &Error{
			Code: EUnauthorized,
			Msg:  "authorizer cannot access document",
		}
	}
}

// TokenAuthorized checks to see if the authorization provided is allowed access the orgs documents.
func TokenAuthorized(a *Authorization) func(ID, DocumentIndex) error {
	return func(id ID, idx DocumentIndex) error {
		if !a.IsActive() {
			return &Error{
				Code: EUnauthorized,
				Msg:  "authorizer cannot access document",
			}
		}

		oids, err := idx.GetDocumentsAccessors(id)
		if err != nil {
			return err
		}
		orgs := map[ID]bool{}
		for _, oid := range oids {
			orgs[oid] = true
		}

		for _, p := range a.Permissions {
			if p.Action == ReadAction {
				continue
			}

			// If the authz has a direct permission to access the resource
			if p.Resource.Type == DocumentsResourceType && p.Resource.ID != nil && *p.Resource.ID == id {
				return nil
			}
			// If the authz has a direct permission to access the class of resources
			if p.Resource.Type == DocumentsResourceType && p.Resource.ID == nil && p.Resource.OrgID == nil {
				return nil
			}

			if p.Resource.Type == DocumentsResourceType && p.Resource.OrgID != nil && orgs[*p.Resource.OrgID] {
				return nil
			}

		}

		return &Error{
			Code: EUnauthorized,
			Msg:  "authorization cannot access document",
		}
	}
}

// WhereOrg retrieves a list of the ids of the documents that belong to the provided org.
func WhereOrg(org string) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return nil, err
		}
		return idx.GetAccessorsDocuments("org", oid)
	}
}

// AuthorizedWhereOrgID ensures that the authorizer is allowed to access the org provideds documents and then
// retrieves a list of the ids of the documents that belong to the provided orgID.
func AuthorizedWhereOrgID(a Authorizer, id ID) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		if err := idx.FindOrganizationByID(id); err != nil {
			return nil, err
		}
		return authorizedWhereOrgID(a, id, idx)
	}
}

func authorizedWhereOrgID(a Authorizer, id ID, idx DocumentIndex) ([]ID, error) {
	var isTokenAuth bool
	switch a.(type) {
	case *Authorization:
		isTokenAuth = true
	}
	if isTokenAuth {
		if !a.Allowed(Permission{
			// TODO(desa): this should be configurable, but should be sufficient for now. In particular this
			// means that tokens cannot be used to delete documents for now if the AuthorizedWhereOrg is called.
			Action: ReadAction,
			Resource: Resource{
				Type:  OrgsResourceType,
				OrgID: &id,
			},
		}) {
			return nil, &Error{
				Code: EUnauthorized,
				Msg:  "authorizer cannot access documents",
			}
		}
	} else {
		if err := idx.IsOrgAccessor(a.GetUserID(), id); err != nil {
			return nil, err
		}
	}
	return idx.GetAccessorsDocuments("org", id)
}

// AuthorizedWhereOrg ensures that the authorizer is allowed to access the org provideds documents and then
// retrieves a list of the ids of the documents that belong to the provided org.
func AuthorizedWhereOrg(a Authorizer, org string) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return nil, err
		}
		return authorizedWhereOrgID(a, oid, idx)
	}
}

// TokenAuthorizedWhereOrg ensures that the authorization is allowed to access the org provideds documents and then
// retrieves a list of the ids of the documents that belong to the provided org.
func TokenAuthorizedWhereOrg(a *Authorization, org string) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		oid, err := idx.FindOrganizationByName(org)
		if err != nil {
			return nil, err
		}
		return authorizedWhereOrgID(a, oid, idx)
	}
}

// AuthorizedWhere retrieves all documents that the user is authorized to access. This is done by
// retrieving the list of all orgs where the user is an accessor and the retrieving all of their
// documents.
func AuthorizedWhere(a Authorizer) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	switch t := a.(type) {
	case *Authorization:
		return TokenAuthorizedWhere(t)
	}

	var ids []ID
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		dids, err := idx.GetAccessorsDocuments("user", a.GetUserID())
		if err != nil {
			return nil, err
		}

		ids = append(ids, dids...)

		orgIDs, err := idx.UsersOrgs(a.GetUserID())
		if err != nil {
			return nil, err
		}

		for _, orgID := range orgIDs {
			dids, err := idx.GetAccessorsDocuments("org", orgID)
			if err != nil {
				return nil, err
			}

			ids = append(ids, dids...)
		}

		// TODO(desa): we should probably dedupe this list eventually.
		return ids, nil
	}
}

// TokenAuthorizedWhere retrieves all documents that the authorization is allowed to access.
func TokenAuthorizedWhere(a *Authorization) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	// TODO(desa): what to do about retrieving all documents using auth? (e.g. write/read:documents/*)
	var ids []ID
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		if !a.IsActive() {
			return nil, &Error{
				Code: EUnauthorized,
				Msg:  "authorizer cannot access documents",
			}
		}

		for _, p := range a.Permissions {
			if p.Resource.Type == DocumentsResourceType && p.Resource.OrgID != nil {
				oids, err := idx.GetAccessorsDocuments("org", *p.Resource.OrgID)
				if err != nil {
					return nil, err
				}
				ids = append(ids, oids...)
			}

			if p.Resource.Type == DocumentsResourceType && p.Resource.ID != nil {
				ids = append(ids, *p.Resource.ID)
			}
		}

		return ids, nil
	}
}

// WhereID passes through the id provided.
func WhereID(docID ID) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		return []ID{docID}, nil
	}
}

// AuthorizedWhereID ensures that the authorizer provided either has the permission to access the document
// or the user associated with the authorizer is an org accessor.
func AuthorizedWhereID(a Authorizer, docID ID) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	switch t := a.(type) {
	case *Authorization:
		return TokenAuthorizedWhereID(t, docID)
	}

	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		oids, err := idx.GetDocumentsAccessors(docID)
		if err != nil {
			return nil, err
		}

		for _, oid := range oids {
			if err := idx.IsOrgAccessor(a.GetUserID(), oid); err == nil {
				return []ID{docID}, nil
			}
		}

		return nil, &Error{
			Code: EUnauthorized,
			Msg:  "authorizer cannot access document",
		}
	}
}

// TokenAuthorizedWhereID ensures that the authorization provided has the permission to access the document.
func TokenAuthorizedWhereID(a *Authorization, docID ID) func(DocumentIndex, DocumentDecorator) ([]ID, error) {
	return func(idx DocumentIndex, _ DocumentDecorator) ([]ID, error) {
		if !a.IsActive() {
			return nil, &Error{
				Code: EUnauthorized,
				Msg:  "authorizer cannot access documents",
			}
		}

		oids, err := idx.GetDocumentsAccessors(docID)
		if err != nil {
			return nil, err
		}
		orgs := map[ID]bool{}
		for _, oid := range oids {
			orgs[oid] = true
		}

		for _, p := range a.Permissions {
			// If the authz has a direct permission to access the resource
			if p.Resource.Type == DocumentsResourceType && p.Resource.ID != nil && docID == *p.Resource.ID {
				return []ID{docID}, nil
			}
			// If the authz has a direct permission to access the class of resources
			if p.Resource.Type == DocumentsResourceType && p.Resource.ID == nil && p.Resource.OrgID == nil {
				return []ID{docID}, nil
			}

			if p.Resource.Type == DocumentsResourceType && p.Resource.OrgID != nil && orgs[*p.Resource.OrgID] {
				return []ID{docID}, nil
			}
		}

		return nil, &Error{
			Code: EUnauthorized,
			Msg:  "authorization cannot access document",
		}
	}
}
