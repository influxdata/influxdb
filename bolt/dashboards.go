package bolt

import (
  "context"

  "github.com/boltdb/bolt"
  "github.com/influxdata/chronograf"
  "github.com/influxdata/chronograf/bolt/internal"
)

var _ chronograf.DashboardsStore = &DashboardsStore{}

type DashboardsStore struct {
  client *Client
}

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
