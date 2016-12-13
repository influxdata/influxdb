package bolt

import (
  "context"

  "github.com/boltdb/bolt"
  "github.com/influxdata/chronograf"
  "github.com/influxdata/chronograf/bolt/internal"
)

var _ chronograf.DashboardsStore = &DashboardsStore{}

var DashboardBucket = []byte("Dashoard")

type DashboardsStore struct {
  client *Client
}

// All returns all known dashboards
func (s *DashboardsStore) All(ctx context.Context) ([]chronograf.Dashboard, error) {
  var srcs []chronograf.Dashboard
  if err := s.client.db.View(func(tx *bolt.Tx) error {
    if err := tx.Bucket(DashboardBucket).ForEach(func(k, v []byte) error {
      var src chonograf.Dashboard
      if err := internal.UnmarshalDashboard(v, &src); err != nil {
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

// Add creates a new Dashboard in the DashboardsStore
func (d *DashboardsStore) Add(ctx context.Context, src chronograf.Dashboard) (chronograf.Dashboard, error) {
  if err := d.client.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(DashboardBucket)
		id, err := d.IDs.Generate()
		if err != nil {
			return err
		}

		src.ID = id
		if v, err := internal.MarshalDashboard(src); err != nil {
			return err
		} else if err := b.Put([]byte(src.ID), v); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.Dashboard{}, err
	}

	return src, nil
}

// Get returns a Dashboard if the id exists.
func (d *DashboardsStore) Get(ctx context.Context, id int) (chronograf.Dashboard, error) {
	var src chronograf.Dashboard
	if err := d.client.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(LayoutBucket).Get([]byte(id)); v == nil {
			return chronograf.ErrDashboardNotFound
		} else if err := internal.UnmarshalDashboard(v, &src); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return chronograf.Dashboard{}, err
	}

	return src, nil
}
