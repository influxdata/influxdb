package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/bolt/internal"
	"golang.org/x/net/context"
)

// Ensure SourcesStore implements mrfusion.SourcesStore.
var _ mrfusion.SourcesStore = &SourcesStore{}

var SourcesBucket = []byte("Sources")

type SourcesStore struct {
	client *Client
}

// All returns all known sources
func (s *SourcesStore) All(ctx context.Context) ([]mrfusion.Source, error) {
	var srcs []mrfusion.Source
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(SourcesBucket).ForEach(func(k, v []byte) error {
			var src mrfusion.Source
			if err := internal.UnmarshalSource(v, &src); err != nil {
				return err
			}
			srcs = append(srcs, src)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return srcs, nil

}

// Add creates a new Source in the SourceStore.
func (s *SourcesStore) Add(ctx context.Context, src mrfusion.Source) (mrfusion.Source, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(SourcesBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		src.ID = int(seq)

		if v, err := internal.MarshalSource(src); err != nil {
			return err
		} else if err := b.Put(itob(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Source{}, err
	}

	return src, nil
}

// Delete removes the Source from the SourcesStore
func (s *SourcesStore) Delete(ctx context.Context, src mrfusion.Source) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(SourcesBucket).Delete(itob(src.ID)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get returns a Source if the id exists.
func (s *SourcesStore) Get(ctx context.Context, id int) (mrfusion.Source, error) {
	var src mrfusion.Source
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(SourcesBucket).Get(itob(id)); v == nil {
			return mrfusion.ErrSourceNotFound
		} else if err := internal.UnmarshalSource(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Source{}, err
	}

	return src, nil
}

// Update a Source
func (s *SourcesStore) Update(ctx context.Context, src mrfusion.Source) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing soource with the same ID.
		b := tx.Bucket(SourcesBucket)
		if v := b.Get(itob(src.ID)); v == nil {
			return mrfusion.ErrSourceNotFound
		}

		if v, err := internal.MarshalSource(src); err != nil {
			return err
		} else if err := b.Put(itob(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
