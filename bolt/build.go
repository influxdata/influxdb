package bolt

import (
	"context"

	"github.com/boltdb/bolt"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure BuildStore struct implements chronograf.BuildStore interface
var _ chronograf.BuildStore = &BuildStore{}

// BuildBucket is the bolt bucket used to store Chronograf build information
var BuildBucket = []byte("Build")

// BuildKey is the constant key used in the bolt bucket
var BuildKey = []byte("build")

// BuildStore is a bolt implementation to store Chronograf build information
type BuildStore struct {
	client *Client
}

// Get retrieves Chronograf build information from the database
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

// Update overwrites the current Chronograf build information in the database
func (s *BuildStore) Update(ctx context.Context, build chronograf.BuildInfo) error {
	if err := s.client.db.Update(func(tx *bolt.Tx) error {
		return s.update(ctx, build, tx)
	}); err != nil {
		return err
	}

	return nil
}

// get retrieves the current build, falling back to a default when missing
func (s *BuildStore) get(ctx context.Context, tx *bolt.Tx) (chronograf.BuildInfo, error) {
	var build chronograf.BuildInfo
	if v := tx.Bucket(BuildBucket).Get(BuildKey); v == nil {
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
	if v, err := internal.MarshalBuild(build); err != nil {
		return err
	} else if err := tx.Bucket(BuildBucket).Put(BuildKey, v); err != nil {
		return err
	}
	return nil
}
