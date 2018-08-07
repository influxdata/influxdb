package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	dashboardBucket = []byte("dashboardsv2")
)

var _ platform.DashboardService = (*Client)(nil)

func (c *Client) initializeDashboards(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(dashboardBucket)); err != nil {
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

	v := tx.Bucket(dashboardBucket).Get([]byte(id))

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
			return bytes.Equal(d.ID, *filter.ID)
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
		d.ID = c.IDGenerator.ID()

		for _, cell := range d.Cells {
			cell.ID = c.IDGenerator.ID()

			if err := c.createViewIfNotExists(ctx, tx, cell, platform.AddDashboardCellOptions{}); err != nil {
				return err
			}
		}

		return c.putDashboard(ctx, tx, d)
	})
}

func (c *Client) createViewIfNotExists(ctx context.Context, tx *bolt.Tx, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	if len(opts.UsingView) != 0 {
		// Creates a hard copy of a view
		v, err := c.findViewByID(ctx, tx, opts.UsingView)
		if err != nil {
			return err
		}
		view, err := c.copyView(ctx, tx, v.ID)
		if err != nil {
			return err
		}
		cell.ViewID = view.ID
		return nil
	} else if len(cell.ViewID) != 0 {
		// Creates a soft copy of a view
		_, err := c.findViewByID(ctx, tx, cell.ViewID)
		if err != nil {
			return err
		}
		return nil
	}

	// If not view exists create the view
	view := &platform.View{}
	if err := c.createView(ctx, tx, view); err != nil {
		return err
	}
	cell.ViewID = view.ID

	return nil
}

// ReplaceDashboardCells creates a platform dashboard and sets d.ID.
func (c *Client) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, id)
		if err != nil {
			return err
		}

		ids := map[string]*platform.Cell{}
		for _, cell := range d.Cells {
			ids[cell.ID.String()] = cell
		}

		for _, cell := range cs {
			if len(cell.ID) == 0 {
				return fmt.Errorf("cannot provide empty cell id")
			}

			cl, ok := ids[cell.ID.String()]
			if !ok {
				return fmt.Errorf("cannot replace cells that were not already present")
			}

			if !bytes.Equal(cl.ViewID, cell.ViewID) {
				return fmt.Errorf("cannot update view id in replace")
			}
		}

		d.Cells = cs

		return c.putDashboard(ctx, tx, d)
	})
}

// AddDashboardCell adds a cell to a dashboard and sets the cells ID.
func (c *Client) AddDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, id)
		if err != nil {
			return err
		}
		cell.ID = c.IDGenerator.ID()
		if err := c.createViewIfNotExists(ctx, tx, cell, opts); err != nil {
			return err
		}

		d.Cells = append(d.Cells, cell)
		return c.putDashboard(ctx, tx, d)
	})
}

// RemoveDashboardCell removes a cell from a dashboard.
func (c *Client) RemoveDashboardCell(ctx context.Context, dashboardID, cellID platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}

		idx := -1
		for i, cell := range d.Cells {
			if bytes.Equal(cell.ID, cellID) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return platform.ErrCellNotFound
		}

		if err := c.deleteView(ctx, tx, d.Cells[idx].ViewID); err != nil {
			return err
		}

		d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)
		return c.putDashboard(ctx, tx, d)
	})
}

// UpdateDashboardCell udpates a cell on a dashboard.
func (c *Client) UpdateDashboardCell(ctx context.Context, dashboardID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	var cell *platform.Cell
	err := c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}

		idx := -1
		for i, cell := range d.Cells {
			if bytes.Equal(cell.ID, cellID) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return platform.ErrCellNotFound
		}

		if err := upd.Apply(d.Cells[idx]); err != nil {
			return err
		}

		cell = d.Cells[idx]

		return c.putDashboard(ctx, tx, d)
	})

	if err != nil {
		return nil, err
	}

	return cell, nil
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
	if err := tx.Bucket(dashboardBucket).Put([]byte(d.ID), v); err != nil {
		return err
	}
	return nil
}

// forEachDashboard will iterate through all dashboards while fn returns true.
func (c *Client) forEachDashboard(ctx context.Context, tx *bolt.Tx, fn func(*platform.Dashboard) bool) error {
	cur := tx.Bucket(dashboardBucket).Cursor()
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

	if err := upd.Apply(d); err != nil {
		return nil, err
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
	d, err := c.findDashboardByID(ctx, tx, id)
	if err != nil {
		return err
	}
	for _, cell := range d.Cells {
		if err := c.deleteView(ctx, tx, cell.ViewID); err != nil {
			return err
		}
	}
	return tx.Bucket(dashboardBucket).Delete([]byte(id))
}
