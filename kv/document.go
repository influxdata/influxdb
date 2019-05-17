package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"github.com/influxdata/influxdb"
)

const (
	documentContentBucket = "/documents/content"
	documentMetaBucket    = "/documents/meta"
)

func (s *Service) initializeDocuments(ctx context.Context, tx Tx) error {
	if _, err := s.createDocumentStore(ctx, tx, "templates"); err != nil {
		return err
	}

	return nil
}

// CreateDocumentStore creates an instance of a document store by instantiating the buckets for the store.
func (s *Service) CreateDocumentStore(ctx context.Context, ns string) (influxdb.DocumentStore, error) {
	// TODO(desa): keep track of which namespaces exist.
	var ds influxdb.DocumentStore

	err := s.kv.Update(ctx, func(tx Tx) error {
		store, err := s.createDocumentStore(ctx, tx, ns)
		if err != nil {
			return err
		}

		ds = store
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ds, nil
}

func (s *Service) createDocumentStore(ctx context.Context, tx Tx, ns string) (influxdb.DocumentStore, error) {
	if _, err := tx.Bucket([]byte(path.Join(ns, documentContentBucket))); err != nil {
		return nil, err
	}

	if _, err := tx.Bucket([]byte(path.Join(ns, documentMetaBucket))); err != nil {
		return nil, err
	}

	return &DocumentStore{
		namespace: ns,
		service:   s,
	}, nil
}

// FindDocumentStore finds the buckets associated with the namespace provided.
func (s *Service) FindDocumentStore(ctx context.Context, ns string) (influxdb.DocumentStore, error) {
	var ds influxdb.DocumentStore

	err := s.kv.View(ctx, func(tx Tx) error {
		if _, err := tx.Bucket([]byte(path.Join(ns, documentContentBucket))); err != nil {
			return err
		}

		if _, err := tx.Bucket([]byte(path.Join(ns, documentMetaBucket))); err != nil {
			return err
		}

		ds = &DocumentStore{
			namespace: ns,
			service:   s,
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ds, nil
}

// DocumentStore implements influxdb.DocumentStore.
type DocumentStore struct {
	service   *Service
	namespace string
}

// CreateDocument creates an instance of a document and sets the ID. After which it applies each of the options provided.
func (s *DocumentStore) CreateDocument(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		err := s.service.createDocument(ctx, tx, s.namespace, d)
		if err != nil {
			return err
		}

		idx := &DocumentIndex{
			service:  s.service,
			tx:       tx,
			ctx:      ctx,
			writable: true,
		}
		for _, opt := range opts {
			if err = opt(d.ID, idx); err != nil {
				return err
			}
		}

		if err = s.decorateDocumentWithLabels(ctx, tx, d); err != nil {
			return err
		}

		return nil
	})
}

// DocumentIndex implements influxdb.DocumentIndex. It is used to access labels/owners of documents.
type DocumentIndex struct {
	service  *Service
	ctx      context.Context
	tx       Tx
	writable bool
}

// AddDocumentLabel creates a label mapping for the label provided.
func (i *DocumentIndex) AddDocumentLabel(docID, labelID influxdb.ID) error {
	m := &influxdb.LabelMapping{
		LabelID:      labelID,
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	if err := i.service.createLabelMapping(i.ctx, i.tx, m); err != nil {
		return err
	}

	return nil
}

// RemoveDocumentLabel removes a label mapping for the label provided.
func (i *DocumentIndex) RemoveDocumentLabel(docID, labelID influxdb.ID) error {
	m := &influxdb.LabelMapping{
		LabelID:      labelID,
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	if err := i.service.deleteLabelMapping(i.ctx, i.tx, m); err != nil {
		return err
	}

	return nil
}

// FindLabelByID retrieves a label by id.
func (i *DocumentIndex) FindLabelByID(id influxdb.ID) error {
	_, err := i.service.findLabelByID(i.ctx, i.tx, id)
	return err
}

// AddDocumentOwner creates a urm for the document id and owner id provided.
func (i *DocumentIndex) AddDocumentOwner(id influxdb.ID, ownerType string, ownerID influxdb.ID) error {
	if err := i.ownerExists(ownerType, ownerID); err != nil {
		return err
	}

	// TODO(desa): support users owning resources.

	m := &influxdb.UserResourceMapping{
		UserID:   ownerID,
		UserType: influxdb.Owner,
		// In this case UserID refers to an organization rather than a user.
		MappingType:  influxdb.OrgMappingType,
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   id,
	}
	return i.service.createUserResourceMapping(i.ctx, i.tx, m)
}

// RemoveDocumentOwner deletes the urm for the document id and owner id provided.
func (i *DocumentIndex) RemoveDocumentOwner(id influxdb.ID, ownerType string, ownerID influxdb.ID) error {
	return i.service.removeDocumentOwner(i.ctx, i.tx, ownerID, id)
}

// WithoutOwners removes all owners from a document. In particular it is used to cleanup urms on document delete.
func WithoutOwners(id influxdb.ID, idx influxdb.DocumentIndex) error {
	ownerIDs, err := idx.GetDocumentsAccessors(id)
	if err != nil {
		return err
	}

	for _, ownerID := range ownerIDs {
		if err := idx.RemoveDocumentOwner(id, "org", ownerID); err != nil {
			return err
		}
	}

	return nil
}

// UsersOrgs retrieves a list of all orgs that a user is an accessor of.
func (i *DocumentIndex) UsersOrgs(userID influxdb.ID) ([]influxdb.ID, error) {
	f := influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
	}

	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ResourceID)
	}

	return ids, nil
}

// IsOrgAccessor checks to see if the user is an accessor of the org provided. If the operation
// is writable it ensures that the user is owner.
func (i *DocumentIndex) IsOrgAccessor(userID influxdb.ID, orgID influxdb.ID) error {
	f := influxdb.UserResourceMappingFilter{
		UserID:       userID,
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   orgID,
	}

	if i.writable {
		f.UserType = influxdb.Owner
	}

	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return err
	}

	for _, m := range ms {
		switch m.UserType {
		case influxdb.Owner, influxdb.Member:
			return nil
		default:
			continue
		}
	}

	return &influxdb.Error{
		Code: influxdb.EUnauthorized,
		Msg:  "user is not org member",
	}
}

func (i *DocumentIndex) ownerExists(ownerType string, ownerID influxdb.ID) error {
	switch ownerType {
	case "org":
		if _, err := i.service.findOrganizationByID(i.ctx, i.tx, ownerID); err != nil {
			return err
		}
	case "user":
		if _, err := i.service.findUserByID(i.ctx, i.tx, ownerID); err != nil {
			return err
		}
	default:
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  fmt.Sprintf("unknown owner type %q", ownerType),
		}
	}

	return nil
}

// FindOrganizationByName retrieves the organization ID of the org provided.
func (i *DocumentIndex) FindOrganizationByName(org string) (influxdb.ID, error) {
	o, err := i.service.findOrganizationByName(i.ctx, i.tx, org)
	if err != nil {
		return influxdb.InvalidID(), err
	}
	return o.ID, nil
}

// FindOrganizationByID checks if the org existence by the org id provided.
func (i *DocumentIndex) FindOrganizationByID(id influxdb.ID) error {
	_, err := i.service.findOrganizationByID(i.ctx, i.tx, id)
	if err != nil {
		return err
	}
	return nil
}

// GetDocumentsAccessors retrieves the list of accessors of a document.
func (i *DocumentIndex) GetDocumentsAccessors(docID influxdb.ID) ([]influxdb.ID, error) {
	f := influxdb.UserResourceMappingFilter{
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	if i.writable {
		f.UserType = influxdb.Owner
	}
	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		if m.MappingType == influxdb.UserMappingType {
			continue
		}
		// TODO(desa): this is really an orgID, eventually we should support users and org as owners of documents
		ids = append(ids, m.UserID)
	}

	return ids, nil
}

// GetAccessorsDocuments retrieves the list of documents a user is allowed to access.
func (i *DocumentIndex) GetAccessorsDocuments(ownerType string, ownerID influxdb.ID) ([]influxdb.ID, error) {
	if err := i.ownerExists(ownerType, ownerID); err != nil {
		return nil, err
	}

	f := influxdb.UserResourceMappingFilter{
		UserID:       ownerID,
		ResourceType: influxdb.DocumentsResourceType,
	}
	if i.writable {
		f.UserType = influxdb.Owner
	}
	ms, err := i.service.findUserResourceMappings(i.ctx, i.tx, f)
	if err != nil {
		return nil, err
	}

	ids := make([]influxdb.ID, 0, len(ms))
	for _, m := range ms {
		ids = append(ids, m.ResourceID)
	}

	return ids, nil
}

func (s *Service) createDocument(ctx context.Context, tx Tx, ns string, d *influxdb.Document) error {
	d.ID = s.IDGenerator.ID()
	d.Meta.CreatedAt = s.Now()
	if err := s.putDocument(ctx, tx, ns, d); err != nil {
		return err
	}

	return nil
}

func (s *Service) putDocument(ctx context.Context, tx Tx, ns string, d *influxdb.Document) error {
	if err := s.putDocumentMeta(ctx, tx, ns, d.ID, d.Meta); err != nil {
		return err
	}

	if err := s.putDocumentContent(ctx, tx, ns, d.ID, d.Content); err != nil {
		return err
	}

	// TODO(desa): index document meta

	return nil
}

func (s *Service) putAtID(ctx context.Context, tx Tx, bucket string, id influxdb.ID, i interface{}) error {
	v, err := json.Marshal(i)
	if err != nil {
		return err
	}

	k, err := id.Encode()
	if err != nil {
		return err
	}

	b, err := tx.Bucket([]byte(bucket))
	if err != nil {
		return err
	}

	if err := b.Put(k, v); err != nil {
		return err
	}

	return nil
}

func (s *Service) putDocumentContent(ctx context.Context, tx Tx, ns string, id influxdb.ID, data interface{}) error {
	return s.putAtID(ctx, tx, path.Join(ns, documentContentBucket), id, data)
}

func (s *Service) putDocumentMeta(ctx context.Context, tx Tx, ns string, id influxdb.ID, m influxdb.DocumentMeta) error {
	return s.putAtID(ctx, tx, path.Join(ns, documentMetaBucket), id, m)
}

func (s *DocumentStore) PutDocument(ctx context.Context, d *influxdb.Document) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		return s.service.putDocument(ctx, tx, s.namespace, d)
	})
}

func (s *Service) findDocumentsByID(ctx context.Context, tx Tx, ns string, ids ...influxdb.ID) ([]*influxdb.Document, error) {
	ds := make([]*influxdb.Document, 0, len(ids))

	for _, id := range ids {
		d, err := s.findDocumentByID(ctx, tx, ns, id)
		if err != nil {
			return nil, err
		}

		ds = append(ds, d)
	}

	return ds, nil
}

func (s *Service) findDocumentByID(ctx context.Context, tx Tx, ns string, id influxdb.ID) (*influxdb.Document, error) {
	m, err := s.findDocumentMetaByID(ctx, tx, ns, id)
	if err != nil {
		return nil, err
	}

	return &influxdb.Document{
		ID:     id,
		Meta:   *m,
		Labels: []*influxdb.Label{},
	}, nil
}

func (s *Service) findByID(ctx context.Context, tx Tx, bucket string, id influxdb.ID, i interface{}) error {
	b, err := tx.Bucket([]byte(bucket))
	if err != nil {
		return err
	}

	k, err := id.Encode()
	if err != nil {
		return err
	}

	v, err := b.Get(k)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(v, i); err != nil {
		return err
	}

	return nil
}

func (s *Service) findDocumentMetaByID(ctx context.Context, tx Tx, ns string, id influxdb.ID) (*influxdb.DocumentMeta, error) {
	m := &influxdb.DocumentMeta{}

	if err := s.findByID(ctx, tx, path.Join(ns, documentMetaBucket), id, m); err != nil {
		return nil, err
	}

	return m, nil
}

func (s *Service) findDocumentContentByID(ctx context.Context, tx Tx, ns string, id influxdb.ID) (interface{}, error) {
	var data interface{}
	if err := s.findByID(ctx, tx, path.Join(ns, documentContentBucket), id, &data); err != nil {
		return nil, err
	}

	return data, nil
}

// DocumentDecorator is used to communication the decoration of documents to the
// document store.
type DocumentDecorator struct {
	data   bool
	labels bool

	writable bool
}

// IncludeContent signals that the document should include its content when returned.
func (d *DocumentDecorator) IncludeContent() error {
	if d.writable {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "cannot include data in document",
		}
	}

	d.data = true

	return nil
}

// IncludeLabels signals that the document should include its labels when returned.
func (d *DocumentDecorator) IncludeLabels() error {
	if d.writable {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "cannot include labels in document",
		}
	}

	d.labels = true

	return nil
}

// FindDocuments retrieves all documenst returned by the document find options.
func (s *DocumentStore) FindDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
	var ds []*influxdb.Document
	err := s.service.kv.View(ctx, func(tx Tx) error {
		if len(opts) == 0 {
			// TODO(desa): might be a better way to do get all.
			if err := s.service.findDocuments(ctx, tx, s.namespace, &ds); err != nil {
				return err
			}

			return nil
		}

		idx := &DocumentIndex{
			service: s.service,
			tx:      tx,
			ctx:     ctx,
		}

		dd := &DocumentDecorator{}

		var ids []influxdb.ID
		for _, opt := range opts {
			is, err := opt(idx, dd)
			if err != nil {
				return err
			}

			ids = append(ids, is...)
		}

		docs, err := s.service.findDocumentsByID(ctx, tx, s.namespace, ids...)
		if err != nil {
			return err
		}

		if dd.data {
			for _, doc := range docs {
				d, err := s.service.findDocumentContentByID(ctx, tx, s.namespace, doc.ID)
				if err != nil {
					return err
				}
				doc.Content = d
			}
		}

		if dd.labels {
			for _, doc := range docs {
				if err := s.decorateDocumentWithLabels(ctx, tx, doc); err != nil {
					return err
				}
			}
		}

		ds = append(ds, docs...)

		return nil
	})

	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrDocumentNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	return ds, nil
}

func (s *Service) findDocuments(ctx context.Context, tx Tx, ns string, ds *[]*influxdb.Document) error {
	metab, err := tx.Bucket([]byte(path.Join(ns, documentMetaBucket)))
	if err != nil {
		return err
	}

	cur, err := metab.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); len(k) != 0; k, v = cur.Next() {
		d := &influxdb.Document{}
		if err := d.ID.Decode(k); err != nil {
			return err
		}

		if err := json.Unmarshal(v, &d.Meta); err != nil {
			return err
		}

		*ds = append(*ds, d)
	}

	return nil
}

// DeleteDocuments removes all documents returned by the options.
func (s *DocumentStore) DeleteDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		idx := &DocumentIndex{
			service:  s.service,
			tx:       tx,
			ctx:      ctx,
			writable: true,
		}
		dd := &DocumentDecorator{writable: true}

		ids := []influxdb.ID{}
		for _, opt := range opts {
			dids, err := opt(idx, dd)
			if err != nil {
				return err
			}

			ids = append(ids, dids...)
		}

		for _, id := range ids {
			// This removed the documents owners
			if err := WithoutOwners(id, idx); err != nil {
				return err
			}

			if err := s.service.deleteDocument(ctx, tx, s.namespace, id); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) removeDocumentOwner(ctx context.Context, tx Tx, ownerID, resourceID influxdb.ID) error {
	filter := influxdb.UserResourceMappingFilter{
		ResourceID: resourceID,
		UserID:     ownerID,
	}

	if err := s.deleteUserResourceMapping(ctx, tx, filter); err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteDocument(ctx context.Context, tx Tx, ns string, id influxdb.ID) error {
	if _, err := s.findDocumentMetaByID(ctx, tx, ns, id); err != nil {
		return err
	}

	if err := s.deleteDocumentMeta(ctx, tx, ns, id); err != nil {
		return err
	}

	if err := s.deleteDocumentContent(ctx, tx, ns, id); err != nil {
		return err
	}

	// TODO(desa): deindex document meta

	return nil
}

func (s *Service) deleteAtID(ctx context.Context, tx Tx, bucket string, id influxdb.ID) error {
	k, err := id.Encode()
	if err != nil {
		return err
	}

	b, err := tx.Bucket([]byte(bucket))
	if err != nil {
		return err
	}

	if err := b.Delete(k); err != nil {
		return err
	}

	return nil
}

func (s *Service) deleteDocumentContent(ctx context.Context, tx Tx, ns string, id influxdb.ID) error {
	return s.deleteAtID(ctx, tx, path.Join(ns, documentContentBucket), id)
}

func (s *Service) deleteDocumentMeta(ctx context.Context, tx Tx, ns string, id influxdb.ID) error {
	return s.deleteAtID(ctx, tx, path.Join(ns, documentMetaBucket), id)
}

// UpdateDocument updates the document.
func (s *DocumentStore) UpdateDocument(ctx context.Context, d *influxdb.Document, opts ...influxdb.DocumentOptions) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		idx := &DocumentIndex{
			service:  s.service,
			tx:       tx,
			ctx:      ctx,
			writable: true,
		}
		for _, opt := range opts {
			if err := opt(d.ID, idx); err != nil {
				return err
			}
		}

		if err := s.service.updateDocument(ctx, tx, s.namespace, d); err != nil {
			return err
		}

		if err := s.decorateDocumentWithLabels(ctx, tx, d); err != nil {
			return err
		}

		return nil
	})
}

func (s *Service) updateDocument(ctx context.Context, tx Tx, ns string, d *influxdb.Document) error {
	// TODO(desa): deindex meta
	d.Meta.UpdatedAt = s.Now()
	if err := s.putDocument(ctx, tx, ns, d); err != nil {
		return err
	}

	return nil
}

func (s *DocumentStore) decorateDocumentWithLabels(ctx context.Context, tx Tx, d *influxdb.Document) error {
	ls := []*influxdb.Label{}
	f := influxdb.LabelMappingFilter{
		ResourceID:   d.ID,
		ResourceType: influxdb.DocumentsResourceType,
	}
	if err := s.service.findResourceLabels(ctx, tx, f, &ls); err != nil {
		return err
	}

	d.Labels = append(d.Labels, ls...)
	return nil
}
