package bolt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	platformcontext "github.com/influxdata/platform/context"
)

var (
	dashboardBucket = []byte("dashboardsv2")
)

// TODO(desa): what do we want these to be?
const (
	dashboardCreatedEvent = "Dashboard Created"
	dashboardUpdatedEvent = "Dashboard Updated"

	dashboardCellsReplacedEvent = "Dashboard Cells Replaced"
	dashboardCellAddedEvent     = "Dashboard Cell Added"
	dashboardCellRemovedEvent   = "Dashboard Cell Removed"
	dashboardCellUpdatedEvent   = "Dashboard Cell Updated"
)

var _ platform.DashboardService = (*Client)(nil)
var _ platform.DashboardOperationLogService = (*Client)(nil)

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
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	v := tx.Bucket(dashboardBucket).Get(encodedID)
	if len(v) == 0 {
		return nil, platform.ErrDashboardNotFound
	}

	var d platform.Dashboard
	if err := json.Unmarshal(v, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// FindDashboard retrieves a dashboard using an arbitrary dashboard filter.
func (c *Client) FindDashboard(ctx context.Context, filter platform.DashboardFilter) (*platform.Dashboard, error) {
	if len(filter.IDs) == 1 {
		return c.FindDashboardByID(ctx, *filter.IDs[0])
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
	if len(filter.IDs) > 0 {
		var sm sync.Map
		for _, id := range filter.IDs {
			sm.Store(id.String(), true)
		}
		return func(d *platform.Dashboard) bool {
			_, ok := sm.Load(d.ID.String())
			return ok
		}
	}

	return func(d *platform.Dashboard) bool { return true }
}

// FindDashboards retrives all dashboards that match an arbitrary dashboard filter.
func (c *Client) FindDashboards(ctx context.Context, filter platform.DashboardFilter, opts platform.FindOptions) ([]*platform.Dashboard, int, error) {
	if len(filter.IDs) == 1 {
		d, err := c.FindDashboardByID(ctx, *filter.IDs[0])
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

	platform.SortDashboards(opts.SortBy, ds)

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

		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCreatedEvent); err != nil {
			return err
		}

		// TODO(desa): don't populate this here. use the first/last methods of the oplog to get meta fields.
		d.Meta.CreatedAt = c.time()

		return c.putDashboardWithMeta(ctx, tx, d)
	})
}

func (c *Client) createViewIfNotExists(ctx context.Context, tx *bolt.Tx, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	if opts.UsingView.Valid() {
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
	} else if cell.ViewID.Valid() {
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

// ReplaceDashboardCells updates the positions of each cell in a dashboard concurrently.
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
			if !cell.ID.Valid() {
				return fmt.Errorf("cannot provide empty cell id")
			}

			cl, ok := ids[cell.ID.String()]
			if !ok {
				return fmt.Errorf("cannot replace cells that were not already present")
			}

			if cl.ViewID != cell.ViewID {
				return fmt.Errorf("cannot update view id in replace")
			}
		}

		d.Cells = cs
		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellsReplacedEvent); err != nil {
			return err
		}

		return c.putDashboardWithMeta(ctx, tx, d)
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

		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellAddedEvent); err != nil {
			return err
		}

		return c.putDashboardWithMeta(ctx, tx, d)
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
			if cell.ID == cellID {
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

		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellRemovedEvent); err != nil {
			return err
		}

		return c.putDashboardWithMeta(ctx, tx, d)
	})
}

// UpdateDashboardCell udpates a cell on a dashboard.
func (c *Client) UpdateDashboardCell(ctx context.Context, dashboardID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	if err := upd.Valid(); err != nil {
		return nil, err
	}

	var cell *platform.Cell
	err := c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return err
		}

		idx := -1
		for i, cell := range d.Cells {
			if cell.ID == cellID {
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

		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellUpdatedEvent); err != nil {
			return err
		}

		return c.putDashboardWithMeta(ctx, tx, d)
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
	encodedID, err := d.ID.Encode()
	if err != nil {
		return err
	}
	if err := tx.Bucket(dashboardBucket).Put(encodedID, v); err != nil {
		return err
	}
	return nil
}

func (c *Client) putDashboardWithMeta(ctx context.Context, tx *bolt.Tx, d *platform.Dashboard) error {
	// TODO(desa): don't populate this here. use the first/last methods of the oplog to get meta fields.
	d.Meta.UpdatedAt = c.time()
	return c.putDashboard(ctx, tx, d)
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
	if err := upd.Valid(); err != nil {
		return nil, err
	}

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

	if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardUpdatedEvent); err != nil {
		return nil, err
	}

	if err := c.putDashboardWithMeta(ctx, tx, d); err != nil {
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
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}
	if err := tx.Bucket(dashboardBucket).Delete(encodedID); err != nil {
		return err
	}
	// TODO(desa): add DeleteKeyValueLog method and use it here.
	return c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		ResourceID:   id,
		ResourceType: platform.DashboardResourceType,
	})
}

const dashboardOperationLogKeyPrefix = "dashboard"

func encodeDashboardOperationLogKey(id platform.ID) ([]byte, error) {
	buf, err := id.Encode()
	if err != nil {
		return nil, err
	}
	return append([]byte(dashboardOperationLogKeyPrefix), buf...), nil
}

// GetDashboardOperationLog retrieves a dashboards operation log.
func (c *Client) GetDashboardOperationLog(ctx context.Context, id platform.ID, opts platform.FindOptions) ([]*platform.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*platform.OperationLogEntry{}

	err := c.db.View(func(tx *bolt.Tx) error {
		key, err := encodeDashboardOperationLogKey(id)
		if err != nil {
			return err
		}

		return c.forEachLogEntry(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &platform.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return log, len(log), nil
}

func (c *Client) appendDashboardEventToLog(ctx context.Context, tx *bolt.Tx, id platform.ID, s string) error {
	e := &platform.OperationLogEntry{
		Description: s,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.
	a, err := platformcontext.GetAuthorizer(ctx)
	if err == nil {
		// Add the user to the log if you can, but don't error if its not there.
		e.UserID = a.GetUserID()
	}

	v, err := json.Marshal(e)
	if err != nil {
		return err
	}

	k, err := encodeDashboardOperationLogKey(id)
	if err != nil {
		return err
	}

	return c.addLogEntry(ctx, tx, k, v, c.time())
}
