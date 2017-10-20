package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure SourcesStore implements chronograf.SourcesStore.
var _ chronograf.SourcesStore = &SourcesStore{}

// SourcesBucket is the bolt bucket used to store source information
var SourcesBucket = []byte("Sources")

// SourcesStore is a bolt implementation to store time-series source information.
type SourcesStore struct {
	client *Client
	Org    string
}

// All returns all known sources
func (s *SourcesStore) All(ctx context.Context) ([]chronograf.Source, error) {
	var srcs []chronograf.Source
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		var err error
		srcs, err = s.all(ctx, tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return srcs, nil

}

// Add creates a new Source in the SourceStore.
func (s *SourcesStore) Add(ctx context.Context, src chronograf.Source) (chronograf.Source, error) {

	// force first source added to be default
	if srcs, err := s.All(ctx); err != nil {
		return chronograf.Source{}, err
	} else if len(srcs) == 0 {
		src.Default = true
	}

	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		return s.add(ctx, &src, tx)
	}); err != nil {
		return chronograf.Source{}, err
	}

	return src, nil
}

// Delete removes the Source from the SourcesStore
func (s *SourcesStore) Delete(ctx context.Context, src chronograf.Source) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := s.setRandomDefault(ctx, src, tx); err != nil {
			return err
		}
		return s.delete(ctx, src, tx)
	}); err != nil {
		return err
	}

	return nil
}

// Get returns a Source if the id exists.
func (s *SourcesStore) Get(ctx context.Context, id int) (chronograf.Source, error) {
	var src chronograf.Source
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		var err error
		src, err = s.get(ctx, id, tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.Source{}, err
	}

	return src, nil
}

// Update a Source
func (s *SourcesStore) Update(ctx context.Context, src chronograf.Source) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		return s.update(ctx, src, tx)
	}); err != nil {
		return err
	}

	return nil
}

func (s *SourcesStore) all(ctx context.Context, tx *bolt.Tx) ([]chronograf.Source, error) {
	var srcs []chronograf.Source
	if err := tx.Bucket(bucket(SourcesBucket, s.Org)).ForEach(func(k, v []byte) error {
		var src chronograf.Source
		if err := internal.UnmarshalSource(v, &src); err != nil {
			return err
		}
		srcs = append(srcs, src)
		return nil
	}); err != nil {
		return srcs, err
	}
	return srcs, nil
}

func (s *SourcesStore) add(ctx context.Context, src *chronograf.Source, tx *bolt.Tx) error {
	b := tx.Bucket(bucket(SourcesBucket, s.Org))
	seq, err := b.NextSequence()
	if err != nil {
		return err
	}
	src.ID = int(seq)

	if src.Default {
		if err := s.resetDefaultSource(ctx, tx); err != nil {
			return err
		}
	}

	if v, err := internal.MarshalSource(*src); err != nil {
		return err
	} else if err := b.Put(itob(src.ID), v); err != nil {
		return err
	}
	return nil
}

func (s *SourcesStore) delete(ctx context.Context, src chronograf.Source, tx *bolt.Tx) error {
	if err := tx.Bucket(bucket(SourcesBucket, s.Org)).Delete(itob(src.ID)); err != nil {
		return err
	}
	return nil
}

func (s *SourcesStore) get(ctx context.Context, id int, tx *bolt.Tx) (chronograf.Source, error) {
	var src chronograf.Source
	if v := tx.Bucket(bucket(SourcesBucket, s.Org)).Get(itob(id)); v == nil {
		return src, chronograf.ErrSourceNotFound
	} else if err := internal.UnmarshalSource(v, &src); err != nil {
		return src, err
	}
	return src, nil
}

func (s *SourcesStore) update(ctx context.Context, src chronograf.Source, tx *bolt.Tx) error {
	// Get an existing soource with the same ID.
	b := tx.Bucket(bucket(SourcesBucket, s.Org))
	if v := b.Get(itob(src.ID)); v == nil {
		return chronograf.ErrSourceNotFound
	}

	if src.Default {
		if err := s.resetDefaultSource(ctx, tx); err != nil {
			return err
		}
	}

	if v, err := internal.MarshalSource(src); err != nil {
		return err
	} else if err := b.Put(itob(src.ID), v); err != nil {
		return err
	}
	return nil
}

// resetDefaultSource unsets the Default flag on all sources
func (s *SourcesStore) resetDefaultSource(ctx context.Context, tx *bolt.Tx) error {
	b := tx.Bucket(bucket(SourcesBucket, s.Org))
	srcs, err := s.all(ctx, tx)
	if err != nil {
		return err
	}

	for _, other := range srcs {
		if other.Default {
			other.Default = false
			if v, err := internal.MarshalSource(other); err != nil {
				return err
			} else if err := b.Put(itob(other.ID), v); err != nil {
				return err
			}
		}
	}
	return nil
}

// setRandomDefault will locate a source other than the provided
// chronograf.Source and set it as the default source. If no other sources are
// available, the provided source will be set to the default source if is not
// already. It assumes that the provided chronograf.Source has been persisted.
func (s *SourcesStore) setRandomDefault(ctx context.Context, src chronograf.Source, tx *bolt.Tx) error {
	// Check if requested source is the current default
	if target, err := s.get(ctx, src.ID, tx); err != nil {
		return err
	} else if target.Default {
		// Locate another source to be the new default
		srcs, err := s.all(ctx, tx)
		if err != nil {
			return err
		}
		var other *chronograf.Source
		for idx := range srcs {
			other = &srcs[idx]
			// avoid selecting the source we're about to delete as the new default
			if other.ID != target.ID {
				break
			}
		}

		// set the other to be the default
		other.Default = true
		if err := s.update(ctx, *other, tx); err != nil {
			return err
		}
	}
	return nil
}
