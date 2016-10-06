package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/bolt/internal"
	"golang.org/x/net/context"
)

// Ensure LayoutStore implements mrfusion.LayoutStore.
var _ mrfusion.LayoutStore = &LayoutStore{}

var LayoutBucket = []byte("Layout")

type LayoutStore struct {
	client *Client
}

// All returns all known layouts
func (s *LayoutStore) All(ctx context.Context) ([]mrfusion.Layout, error) {
	var srcs []mrfusion.Layout
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(LayoutBucket).ForEach(func(k, v []byte) error {
			var src mrfusion.Layout
			if err := internal.UnmarshalLayout(v, &src); err != nil {
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

// Add creates a new Layout in the LayoutStore.
func (s *LayoutStore) Add(ctx context.Context, src mrfusion.Layout) (mrfusion.Layout, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(LayoutBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		src.ID = int(seq)

		if v, err := internal.MarshalLayout(src); err != nil {
			return err
		} else if err := b.Put(itob(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Layout{}, err
	}

	return src, nil
}

// Delete removes the Layout from the LayoutStore
func (s *LayoutStore) Delete(ctx context.Context, src mrfusion.Layout) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(LayoutBucket).Delete(itob(src.ID)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get returns a Layout if the id exists.
func (s *LayoutStore) Get(ctx context.Context, id int) (mrfusion.Layout, error) {
	var src mrfusion.Layout
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(LayoutBucket).Get(itob(id)); v == nil {
			return mrfusion.ErrLayoutNotFound
		} else if err := internal.UnmarshalLayout(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Layout{}, err
	}

	return src, nil
}

// Update a Layout
func (s *LayoutStore) Update(ctx context.Context, src mrfusion.Layout) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing layout with the same ID.
		b := tx.Bucket(LayoutBucket)
		if v := b.Get(itob(src.ID)); v == nil {
			return mrfusion.ErrLayoutNotFound
		}

		if v, err := internal.MarshalLayout(src); err != nil {
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
