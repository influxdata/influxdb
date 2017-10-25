package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure ServersStore implements chronograf.ServersStore.
var _ chronograf.ServersStore = &ServersStore{}

// ServersBucket is the bolt bucket to store lists of servers
var ServersBucket = []byte("Servers")

// ServersStore is the bolt implementation to store servers in a store.
// Used store servers that are associated in some way with a source
type ServersStore struct {
	client *Client
}

// All returns all known servers
func (s *ServersStore) All(ctx context.Context) ([]chronograf.Server, error) {
	var srcs []chronograf.Server
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

// Add creates a new Server in the ServerStore.
func (s *ServersStore) Add(ctx context.Context, src chronograf.Server) (chronograf.Server, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ServersBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		src.ID = int(seq)

		// make the newly added source "active"
		s.resetActiveServer(ctx, tx)
		src.Active = true

		if v, err := internal.MarshalServer(src); err != nil {
			return err
		} else if err := b.Put(itob(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.Server{}, err
	}

	return src, nil
}

// Delete removes the Server from the ServersStore
func (s *ServersStore) Delete(ctx context.Context, src chronograf.Server) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(ServersBucket).Delete(itob(src.ID)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Get returns a Server if the id exists.
func (s *ServersStore) Get(ctx context.Context, id int) (chronograf.Server, error) {
	var src chronograf.Server
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(ServersBucket).Get(itob(id)); v == nil {
			return chronograf.ErrServerNotFound
		} else if err := internal.UnmarshalServer(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.Server{}, err
	}

	return src, nil
}

// Update a Server
func (s *ServersStore) Update(ctx context.Context, src chronograf.Server) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing server with the same ID.
		b := tx.Bucket(ServersBucket)
		if v := b.Get(itob(src.ID)); v == nil {
			return chronograf.ErrServerNotFound
		}

		// only one server can be active at a time
		if src.Active {
			s.resetActiveServer(ctx, tx)
		}

		if v, err := internal.MarshalServer(src); err != nil {
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

func (s *ServersStore) all(ctx context.Context, tx *bolt.Tx) ([]chronograf.Server, error) {
	var srcs []chronograf.Server
	if err := tx.Bucket(ServersBucket).ForEach(func(k, v []byte) error {
		var src chronograf.Server
		if err := internal.UnmarshalServer(v, &src); err != nil {
			return err
		}
		srcs = append(srcs, src)
		return nil
	}); err != nil {
		return srcs, err
	}
	return srcs, nil
}

// resetActiveServer unsets the Active flag on all sources
func (s *ServersStore) resetActiveServer(ctx context.Context, tx *bolt.Tx) error {
	b := tx.Bucket(ServersBucket)
	srcs, err := s.all(ctx, tx)
	if err != nil {
		return err
	}

	for _, other := range srcs {
		if other.Active {
			other.Active = false
			if v, err := internal.MarshalServer(other); err != nil {
				return err
			} else if err := b.Put(itob(other.ID), v); err != nil {
				return err
			}
		}
	}
	return nil
}
