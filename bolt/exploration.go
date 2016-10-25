package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure ExplorationStore implements chronograf.ExplorationStore.
var _ chronograf.ExplorationStore = &ExplorationStore{}

var ExplorationBucket = []byte("Explorations")

type ExplorationStore struct {
	client *Client
}

// Search the ExplorationStore for all explorations owned by userID.
func (s *ExplorationStore) Query(ctx context.Context, uid chronograf.UserID) ([]*chronograf.Exploration, error) {
	var explorations []*chronograf.Exploration
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(ExplorationBucket).ForEach(func(k, v []byte) error {
			var e chronograf.Exploration
			if err := internal.UnmarshalExploration(v, &e); err != nil {
				return err
			} else if e.UserID != uid {
				return nil
			}
			explorations = append(explorations, &e)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return explorations, nil
}

// Create a new Exploration in the ExplorationStore.
func (s *ExplorationStore) Add(ctx context.Context, e *chronograf.Exploration) (*chronograf.Exploration, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ExplorationBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		e.ID = chronograf.ExplorationID(seq)
		e.CreatedAt = s.client.Now().UTC()
		e.UpdatedAt = e.CreatedAt

		if v, err := internal.MarshalExploration(e); err != nil {
			return err
		} else if err := b.Put(itob(int(e.ID)), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return e, nil
}

// Delete the exploration from the ExplorationStore
func (s *ExplorationStore) Delete(ctx context.Context, e *chronograf.Exploration) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(ExplorationBucket).Delete(itob(int(e.ID))); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Retrieve an exploration for an id exists.
func (s *ExplorationStore) Get(ctx context.Context, id chronograf.ExplorationID) (*chronograf.Exploration, error) {
	var e chronograf.Exploration
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(ExplorationBucket).Get(itob(int(id))); v == nil {
			return chronograf.ErrExplorationNotFound
		} else if err := internal.UnmarshalExploration(v, &e); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &e, nil
}

// Update an exploration; will also update the `UpdatedAt` time.
func (s *ExplorationStore) Update(ctx context.Context, e *chronograf.Exploration) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Retreive an existing exploration with the same exploration ID.
		var ee chronograf.Exploration
		b := tx.Bucket(ExplorationBucket)
		if v := b.Get(itob(int(e.ID))); v == nil {
			return chronograf.ErrExplorationNotFound
		} else if err := internal.UnmarshalExploration(v, &ee); err != nil {
			return err
		}

		ee.Name = e.Name
		ee.UserID = e.UserID
		ee.Data = e.Data
		ee.UpdatedAt = s.client.Now().UTC()

		if v, err := internal.MarshalExploration(&ee); err != nil {
			return err
		} else if err := b.Put(itob(int(ee.ID)), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}
