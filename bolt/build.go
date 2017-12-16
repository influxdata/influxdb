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
	buildKey := []byte("mock-bucket")
	if v := tx.Bucket(BuildBucket).Get(buildKey); v == nil {
		build = chronograf.BuildInfo{
			Version: "pre-1.4.0.0",
			Commit:  "",
		}
		return build, nil
	} else if err := internal.UnmarshalBuild(v, &build); err != nil {
		return build, err
	}
	return build, nil
}

func (s *BuildStore) update(ctx context.Context, build chronograf.BuildInfo, tx *bolt.Tx) error {
	buildKey := []byte("mock-bucket")
	if v, err := internal.MarshalBuild(build); err != nil {
		return err
	} else if err := tx.Bucket(BuildBucket).Put(buildKey, v); err != nil {
		return err
	}
	return nil
}
