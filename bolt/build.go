package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

var _ chronograf.BuildStore = &BuildStore{}

var BuildBucket = []byte("Build")

type BuildStore struct {
	client *Client
}

// Get returns a Source if the id exists.
func (s *BuildStore) Get(ctx context.Context) (chronograf.BuildInfo, error) {
	var build chronograf.BuildInfo
	if err := s.client.db.View(func(tx *bolt.Tx) error {
		var err error
		build, err = s.get(ctx, tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.BuildInfo{}, err
	}

	return build, nil
}

// Update a Source
func (s *BuildStore) Update(ctx context.Context, build chronograf.BuildInfo) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		return s.update(ctx, build, tx)
	}); err != nil {
		return err
	}

	return nil
}

func (s *BuildStore) get(ctx context.Context, tx *bolt.Tx) (chronograf.BuildInfo, error) {
	var build chronograf.BuildInfo
	if v := tx.Bucket(BuildBucket).Get(); v == nil {
		return build, chronograf.ErrSourceNotFound
	} else if err := internal.UnmarshalSource(v, &build); err != nil {
		return build, err
	}
	return build, nil
}

func (s *BuildStore) update(ctx context.Context, build chronograf.BuildInfo, tx *bolt.Tx) error {
	// Get an existing soource with the same ID.
	b := tx.Bucket(BuildBucket)
	if v := b.Get(); v == nil {
		return chronograf.ErrSourceNotFound
	}

	if v, err := internal.MarshalSource(build); err != nil {
		return err
	} else if err := b.Put(v); err != nil {
		return err
	}
	return nil
}
