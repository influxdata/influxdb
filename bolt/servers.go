package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/bolt/internal"
	"golang.org/x/net/context"
)

// Ensure ServersStore implements mrfusion.ServersStore.
var _ mrfusion.ServersStore = &ServersStore{}

var ServersBucket = []byte("Servers")

type ServersStore struct {
	client *Client
}

// All returns all known servers
func (s *ServersStore) All(ctx context.Context) ([]mrfusion.Server, error) {
	var srcs []mrfusion.Server
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket(ServersBucket).ForEach(func(k, v []byte) error {
			var src mrfusion.Server
			if err := internal.UnmarshalServer(v, &src); err != nil {
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

// Add creates a new Server in the ServerStore.
func (s *ServersStore) Add(ctx context.Context, src mrfusion.Server) (mrfusion.Server, error) {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ServersBucket)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		src.ID = int(seq)

		if v, err := internal.MarshalServer(src); err != nil {
			return err
		} else if err := b.Put(itob(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Server{}, err
	}

	return src, nil
}

// Delete removes the Server from the ServersStore
func (s *ServersStore) Delete(ctx context.Context, src mrfusion.Server) error {
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
func (s *ServersStore) Get(ctx context.Context, id int) (mrfusion.Server, error) {
	var src mrfusion.Server
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(ServersBucket).Get(itob(id)); v == nil {
			return mrfusion.ErrServerNotFound
		} else if err := internal.UnmarshalServer(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return mrfusion.Server{}, err
	}

	return src, nil
}

// Update a Server
func (s *ServersStore) Update(ctx context.Context, src mrfusion.Server) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		// Get an existing server with the same ID.
		b := tx.Bucket(ServersBucket)
		if v := b.Get(itob(src.ID)); v == nil {
			return mrfusion.ErrServerNotFound
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
