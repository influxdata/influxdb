package bolt

import (
	"context"
	"encoding/json"
	"strconv"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/platform/chronograf/v2"
)

var (
	dashboardV2Bucket = []byte("dashboardsv3")
)

var _ platform.DashboardService = (*Client)(nil)

func (c *Client) initializeDashboards(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(dashboardV2Bucket)); err != nil {
		return err
	}
	return nil
}

// FindDashboardByID retrieves a dashboard by id.
func (c *Client) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	var d *platform.Dashboard

	err := c.db.View(func(tx *bolt.Tx) error {
		dash, err := c.findDashboardByID(ctx, tx, id)
		if err != nil {
			return err
		}
		d = dash
		return nil
	})

	if err != nil {
		return nil, err
	}

	return d, nil
}

func (c *Client) findDashboardByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Dashboard, error) {
	var d platform.Dashboard

	v := tx.Bucket(dashboardV2Bucket).Get([]byte(id))

	if len(v) == 0 {
		return nil, platform.ErrDashboardNotFound
	}

	if err := json.Unmarshal(v, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// FindDashboard retrieves a dashboard using an arbitrary dashboard filter.
func (c *Client) FindDashboard(ctx context.Context, filter platform.DashboardFilter) (*platform.Dashboard, error) {
	if filter.ID != nil {
		return c.FindDashboardByID(ctx, *filter.ID)
	}

	var d *platform.Dashboard
	err := c.db.View(func(tx *bolt.Tx) error {
		filterFn := filterDashboardsFn(filter)
		return c.forEachDashboard(ctx, tx, func(dash *platform.Dashboard) bool {
			if filterFn(dash) {
				d = dash
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, err
	}

	if d == nil {
		return nil, platform.ErrDashboardNotFound
	}

	return d, nil
}

func filterDashboardsFn(filter platform.DashboardFilter) func(d *platform.Dashboard) bool {
	if filter.ID != nil {
		return func(d *platform.Dashboard) bool {
			return d.ID == *filter.ID
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

// FindDashboards retrives all dashboards that match an arbitrary dashboard filter.
func (c *Client) FindDashboards(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
	if filter.ID != nil {
		d, err := c.FindDashboardByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Dashboard{d}, 1, nil
	}

	ds := []*platform.Dashboard{}
	err := c.db.View(func(tx *bolt.Tx) error {
		dashs, err := c.findDashboards(ctx, tx, filter)
		if err != nil {
			return err
		}
		ds = dashs
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return ds, len(ds), nil
}

func (c *Client) findDashboards(ctx context.Context, tx *bolt.Tx, filter platform.DashboardFilter) ([]*platform.Dashboard, error) {
	ds := []*platform.Dashboard{}

	filterFn := filterDashboardsFn(filter)
	err := c.forEachDashboard(ctx, tx, func(d *platform.Dashboard) bool {
		if filterFn(d) {
			ds = append(ds, d)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return ds, nil
}

// CreateDashboard creates a platform dashboard and sets d.ID.
func (c *Client) CreateDashboard(ctx context.Context, d *platform.Dashboard) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		id, err := tx.Bucket(dashboardV2Bucket).NextSequence()
		if err != nil {
			return err
		}
		d.ID = platform.ID(strconv.Itoa(int(id)))

		return c.putDashboard(ctx, tx, d)
	})
}

// PutDashboard will put a dashboard without setting an ID.
func (c *Client) PutDashboard(ctx context.Context, d *platform.Dashboard) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putDashboard(ctx, tx, d)
	})
}

func (c *Client) putDashboard(ctx context.Context, tx *bolt.Tx, d *platform.Dashboard) error {
	v, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := tx.Bucket(dashboardV2Bucket).Put([]byte(d.ID), v); err != nil {
		return err
	}
	return nil
}

// forEachDashboard will iterate through all dashboards while fn returns true.
func (c *Client) forEachDashboard(ctx context.Context, tx *bolt.Tx, fn func(*platform.Dashboard) bool) error {
	cur := tx.Bucket(dashboardV2Bucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		d := &platform.Dashboard{}
		if err := json.Unmarshal(v, d); err != nil {
			return err
		}
		if !fn(d) {
			break
		}
	}

	return nil
}

// UpdateDashboard updates a dashboard according the parameters set on upd.
func (c *Client) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	var d *platform.Dashboard
	err := c.db.Update(func(tx *bolt.Tx) error {
		dash, err := c.updateDashboard(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		d = dash
		return nil
	})

	return d, err
}

func (c *Client) updateDashboard(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	d, err := c.findDashboardByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		d.Name = *upd.Name
	}

	if upd.Cells != nil {
		d.Cells = upd.Cells
	}

	if err := c.putDashboard(ctx, tx, d); err != nil {
		return nil, err
	}

	return d, nil
}

// DeleteDashboard deletes a dashboard and prunes it from the index.
func (c *Client) DeleteDashboard(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteDashboard(ctx, tx, id)
	})
}

func (c *Client) deleteDashboard(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	_, err := c.findDashboardByID(ctx, tx, id)
	if err != nil {
		return err
	}
	return tx.Bucket(dashboardV2Bucket).Delete([]byte(id))
}
