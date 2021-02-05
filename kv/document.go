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

// DocumentStore implements influxdb.DocumentStore.
type DocumentStore struct {
	service   *Service
	namespace string
}

// CreateDocumentStore creates an instance of a document store by instantiating the buckets for the store.
func (s *Service) CreateDocumentStore(ctx context.Context, ns string) (influxdb.DocumentStore, error) {
	// TODO(desa): keep track of which namespaces exist.
	return s.createDocumentStore(ctx, ns)
}

func (s *Service) createDocumentStore(ctx context.Context, ns string) (influxdb.DocumentStore, error) {
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
		err := s.service.createDocument(ctx, tx, s.namespace, d)
		if err != nil {
			return err
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
func (s *DocumentStore) FindDocuments(ctx context.Context, _ influxdb.ID) ([]*influxdb.Document, error) {
	var ds []*influxdb.Document

	err := s.service.kv.View(ctx, func(tx Tx) error {
		// TODO(desa): might be a better way to do get all.
		if err := s.service.findDocuments(ctx, tx, s.namespace, &ds); err != nil {
			return err
		}

		return nil
	})

	return ds, err
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
