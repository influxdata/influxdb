package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	dashboardBucket = []byte("dashboardsv1")
)

func (c *Client) initializeDashboards(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(dashboardBucket)); err != nil {
		return err
	}
	return nil
}

func (c *Client) setOrganizationOnDashboard(ctx context.Context, tx *bolt.Tx, d *platform.Dashboard) error {
	o, err := c.findOrganizationByID(ctx, tx, d.OrganizationID)
	if err != nil {
		return err
	}
	d.Organization = o.Name
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

	v := tx.Bucket(dashboardBucket).Get(id)

	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("dashboard not found")
	}

	if err := json.Unmarshal(v, &d); err != nil {
		return nil, err
	}

	if err := c.setOrganizationOnDashboard(ctx, tx, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// FindDashboard retrieves a dashboard using an arbitrary dashboard filter.
// Filters using ID, or OrganizationID and dashboard Name should be efficient.
// Other filters will do a linear scan across dashboards until it finds a match.
func (c *Client) FindDashboard(ctx context.Context, filter platform.DashboardFilter) (*platform.Dashboard, error) {
	if filter.ID != nil {
		return c.FindDashboardByID(ctx, *filter.ID)
	}

	var d *platform.Dashboard
	err := c.db.View(func(tx *bolt.Tx) error {
		if filter.Organization != nil {
			o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
			if err != nil {
				return err
			}
			filter.OrganizationID = &o.ID
		}

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
		return nil, fmt.Errorf("dashboard not found")
	}

	return d, nil
}

func filterDashboardsFn(filter platform.DashboardFilter) func(d *platform.Dashboard) bool {
	if filter.ID != nil {
		return func(d *platform.Dashboard) bool {
			return bytes.Equal(d.ID, *filter.ID)
		}
	}

	if filter.OrganizationID != nil {
		return func(d *platform.Dashboard) bool {
			return bytes.Equal(d.OrganizationID, *filter.OrganizationID)
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

// FindDashboardsByOrganizationID retrieves all dashboards that belong to a particular organization ID.
func (c *Client) FindDashboardsByOrganizationID(ctx context.Context, orgID platform.ID) ([]*platform.Dashboard, int, error) {
	return c.FindDashboards(ctx, platform.DashboardFilter{OrganizationID: &orgID})
}

// FindDashboardsByOrganizationName retrieves all dashboards that belong to a particular organization.
func (c *Client) FindDashboardsByOrganizationName(ctx context.Context, org string) ([]*platform.Dashboard, int, error) {
	return c.FindDashboards(ctx, platform.DashboardFilter{Organization: &org})
}

// FindDashboards retrives all dashboards that match an arbitrary dashboard filter.
// Filters using ID, or OrganizationID and dashboard Name should be efficient.
// Other filters will do a linear scan across all dashboards searching for a match.
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
	if filter.Organization != nil {
		o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		filter.OrganizationID = &o.ID
	}

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
		if len(d.OrganizationID) == 0 {
			o, err := c.findOrganizationByName(ctx, tx, d.Organization)
			if err != nil {
				return err
			}
			d.OrganizationID = o.ID
		}

		d.ID = c.IDGenerator.ID()

		for i, cell := range d.Cells {
			cell.ID = c.IDGenerator.ID()
			d.Cells[i] = cell
		}

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
	d.Organization = ""
	v, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := tx.Bucket(dashboardBucket).Put(d.ID, v); err != nil {
		return err
	}
	return c.setOrganizationOnDashboard(ctx, tx, d)
}

// forEachDashboard will iterate through all dashboards while fn returns true.
func (c *Client) forEachDashboard(ctx context.Context, tx *bolt.Tx, fn func(*platform.Dashboard) bool) error {
	cur := tx.Bucket(dashboardBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		d := &platform.Dashboard{}
		if err := json.Unmarshal(v, d); err != nil {
			return err
		}
		if err := c.setOrganizationOnDashboard(ctx, tx, d); err != nil {
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

	if err := c.putDashboard(ctx, tx, d); err != nil {
		return nil, err
	}

	if err := c.setOrganizationOnDashboard(ctx, tx, d); err != nil {
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
	return tx.Bucket(dashboardBucket).Delete(id)
}

// AddDashboardCell adds a cell to a dashboard.
func (c *Client) AddDashboardCell(ctx context.Context, dashboardID platform.ID, cell *platform.DashboardCell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}
		cell.ID = c.IDGenerator.ID()
		d.Cells = append(d.Cells, *cell)
		return c.putDashboard(ctx, tx, d)
	})
}

// ReplaceDashboardCell updates a cell in a dashboard.
func (c *Client) ReplaceDashboardCell(ctx context.Context, dashboardID platform.ID, dc *platform.DashboardCell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}
		idx := -1
		for i, cell := range d.Cells {
			if bytes.Equal(dc.ID, cell.ID) {
				idx = i
				break
			}
		}

		if idx == -1 {
			return fmt.Errorf("cell not found")
		}

		d.Cells[idx] = *dc

		return c.putDashboard(ctx, tx, d)
	})
}

// RemoveDashboardCell removes a cell from a dashboard.
func (c *Client) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}
		idx := -1
		for i, cell := range d.Cells {
			if bytes.Equal(cellID, cell.ID) {
				idx = i
				break
			}
		}

		if idx == -1 {
			return fmt.Errorf("cell not found")
		}

		// Remove cell
		d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)

		return c.putDashboard(ctx, tx, d)
	})
}
