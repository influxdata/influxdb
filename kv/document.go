package kv

import (
	"context"
	"encoding/json"
	"path"

	"github.com/influxdata/influxdb/v2"
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

// DocumentStore implements influxdb.DocumentStore.
type DocumentStore struct {
	service   *Service
	namespace string
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

// CreateDocument creates an instance of a document and sets the ID. After which it applies each of the options provided.
func (s *DocumentStore) CreateDocument(ctx context.Context, d *influxdb.Document) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		// Check that labels exist before creating the document.
		// Mapping creation would check for that, but cannot anticipate that until we
		// have a valid document ID.
		for _, l := range d.Labels {
			if _, err := s.service.findLabelByID(ctx, tx, l.ID); err != nil {
				return err
			}
		}

		err := s.service.createDocument(ctx, tx, s.namespace, d)
		if err != nil {
			return err
		}

		for orgID := range d.Organizations {
			if err := s.service.addDocumentOwner(ctx, tx, orgID, d.ID); err != nil {
				return err
			}
		}

		for _, l := range d.Labels {
			if err := s.addDocumentLabelMapping(ctx, tx, d.ID, l.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) createDocument(ctx context.Context, tx Tx, ns string, d *influxdb.Document) error {
	d.ID = s.IDGenerator.ID()
	d.Meta.CreatedAt = s.Now()
	d.Meta.UpdatedAt = s.Now()
	return s.putDocument(ctx, tx, ns, d)
}

// Affo: the only resource in which a org owns something and not a precise user.
func (s *Service) addDocumentOwner(ctx context.Context, tx Tx, orgID influxdb.ID, docID influxdb.ID) error {
	// In this case UserID refers to an organization rather than a user.
	m := &influxdb.UserResourceMapping{
		UserID:       orgID,
		UserType:     influxdb.Owner,
		MappingType:  influxdb.OrgMappingType,
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	return s.createUserResourceMapping(ctx, tx, m)
}

func (s *DocumentStore) addDocumentLabelMapping(ctx context.Context, tx Tx, docID, labelID influxdb.ID) error {
	m := &influxdb.LabelMapping{
		LabelID:      labelID,
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	if err := s.service.createLabelMapping(ctx, tx, m); err != nil {
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

// FindDocument retrieves the specified document with all its content and labels.
func (s *DocumentStore) FindDocument(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
	var d *influxdb.Document
	err := s.service.kv.View(ctx, func(tx Tx) error {
		m, err := s.service.findDocumentMetaByID(ctx, tx, s.namespace, id)
		if err != nil {
			return err
		}
		c, err := s.service.findDocumentContentByID(ctx, tx, s.namespace, id)
		if err != nil {
			return err
		}
		d = &influxdb.Document{
			ID:      id,
			Meta:    *m,
			Content: c,
		}
		if err := s.decorateDocumentWithLabels(ctx, tx, d); err != nil {
			return err
		}
		if err := s.decorateDocumentWithOrgs(ctx, tx, d); err != nil {
			return err
		}

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

	return d, nil
}

// FindDocuments retrieves all documents returned by the document find options.
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
			service:   s.service,
			namespace: s.namespace,
			tx:        tx,
			ctx:       ctx,
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

		if dd.orgs {
			for _, doc := range docs {
				if err := s.decorateDocumentWithOrgs(ctx, tx, doc); err != nil {
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

	cur, err := metab.ForwardCursor(nil)
	if err != nil {
		return err
	}

	for k, v := cur.Next(); len(k) != 0; k, v = cur.Next() {
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

func (s *Service) getDocumentsAccessors(ctx context.Context, tx Tx, docID influxdb.ID) (map[influxdb.ID]influxdb.UserType, error) {
	f := influxdb.UserResourceMappingFilter{
		ResourceType: influxdb.DocumentsResourceType,
		ResourceID:   docID,
	}
	ms, err := s.findUserResourceMappings(ctx, tx, f)
	if err != nil {
		return nil, err
	}
	// The only URM created when creating a document is
	// from an org to the document (not a user).
	orgs := make(map[influxdb.ID]influxdb.UserType, len(ms))
	for _, m := range ms {
		if m.MappingType == influxdb.OrgMappingType {
			orgs[m.UserID] = m.UserType
		}
	}
	return orgs, nil
}

// DeleteDocument removes the specified document.
func (s *DocumentStore) DeleteDocument(ctx context.Context, id influxdb.ID) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		if err := s.service.deleteDocument(ctx, tx, s.namespace, id); err != nil {
			if IsNotFound(err) {
				return &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  influxdb.ErrDocumentNotFound,
				}
			}
			return err
		}
		return nil
	})
}

// DeleteDocuments removes all documents returned by the options.
func (s *DocumentStore) DeleteDocuments(ctx context.Context, opts ...influxdb.DocumentFindOptions) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
		idx := &DocumentIndex{
			service:   s.service,
			namespace: s.namespace,
			tx:        tx,
			ctx:       ctx,
			writable:  true,
		}
		dd := &DocumentDecorator{writable: true}

		var ids []influxdb.ID
		for _, opt := range opts {
			dids, err := opt(idx, dd)
			if err != nil {
				return err
			}

			ids = append(ids, dids...)
		}

		for _, id := range ids {
			if err := s.service.deleteDocument(ctx, tx, s.namespace, id); err != nil {
				if IsNotFound(err) {
					return &influxdb.Error{
						Code: influxdb.ENotFound,
						Msg:  influxdb.ErrDocumentNotFound,
					}
				}
				return err
			}
		}
		return nil
	})
}

func (s *Service) removeDocumentAccess(ctx context.Context, tx Tx, orgID, docID influxdb.ID) error {
	filter := influxdb.UserResourceMappingFilter{
		ResourceID: docID,
		UserID:     orgID,
	}
	if err := s.deleteUserResourceMapping(ctx, tx, filter); err != nil {
		return err
	}
	return nil
}

func (s *Service) deleteDocument(ctx context.Context, tx Tx, ns string, id influxdb.ID) error {
	// Delete mappings.
	orgs, err := s.getDocumentsAccessors(ctx, tx, id)
	if err != nil {
		return err
	}
	for orgID := range orgs {
		if err := s.removeDocumentAccess(ctx, tx, orgID, id); err != nil {
			return err
		}
	}

	// Delete document.
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
func (s *DocumentStore) UpdateDocument(ctx context.Context, d *influxdb.Document) error {
	return s.service.kv.Update(ctx, func(tx Tx) error {
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
	var ls []*influxdb.Label
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

func (s *DocumentStore) decorateDocumentWithOrgs(ctx context.Context, tx Tx, d *influxdb.Document) error {
	// If the orgs are already there, then this is a nop.
	if len(d.Organizations) > 0 {
		return nil
	}
	orgs, err := s.service.getDocumentsAccessors(ctx, tx, d.ID)
	if err != nil {
		return err
	}
	d.Organizations = orgs
	return nil
}
