package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	platformcontext "github.com/influxdata/platform/context"
)

var (
	dashboardBucket         = []byte("dashboardsv2")
	dashboardCellViewBucket = []byte("dashboardcellviewsv1")
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
	if _, err := tx.CreateBucketIfNotExists([]byte(dashboardCellViewBucket)); err != nil {
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
		return nil, &platform.Error{
			Op:  getOp(platform.OpFindDashboardByID),
			Err: err,
		}
	}

	return d, nil
}

func (c *Client) findDashboardByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Dashboard, *platform.Error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	v := tx.Bucket(dashboardBucket).Get(encodedID)
	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrDashboardNotFound,
		}

	}

	var d platform.Dashboard
	if err := json.Unmarshal(v, &d); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
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
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrDashboardNotFound,
		}
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
	ds := []*platform.Dashboard{}
	if len(filter.IDs) == 1 {
		d, err := c.FindDashboardByID(ctx, *filter.IDs[0])
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return ds, 0, &platform.Error{
				Err: err,
				Op:  getOp(platform.OpFindDashboardByID),
			}
		}
		if d == nil {
			return ds, 0, nil
		}
		return []*platform.Dashboard{d}, 1, nil
	}
	err := c.db.View(func(tx *bolt.Tx) error {
		dashs, err := c.findDashboards(ctx, tx, filter)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return err
		}
		ds = dashs
		return nil
	})

	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
			Op:  getOp(platform.OpFindDashboards),
		}
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
	err := c.db.Update(func(tx *bolt.Tx) error {
		d.ID = c.IDGenerator.ID()

		for _, cell := range d.Cells {
			cell.ID = c.IDGenerator.ID()

			if err := c.createCellView(ctx, tx, d.ID, cell.ID, nil); err != nil {
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
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  getOp(platform.OpCreateDashboard),
		}
	}
	return nil
}

func (c *Client) createCellView(ctx context.Context, tx *bolt.Tx, dashID, cellID platform.ID, view *platform.View) error {
	if view == nil {
		// If not view exists create the view
		view = &platform.View{}
	}
	// TODO: this is temporary until we can fully remove the view service.
	view.ID = cellID
	return c.putDashboardCellView(ctx, tx, dashID, cellID, view)
}

// ReplaceDashboardCells updates the positions of each cell in a dashboard concurrently.
func (c *Client) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
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
				return &platform.Error{
					Code: platform.EInvalid,
					Msg:  "cannot provide empty cell id",
				}
			}

			if _, ok := ids[cell.ID.String()]; !ok {
				return &platform.Error{
					Code: platform.EConflict,
					Msg:  "cannot replace cells that were not already present",
				}
			}
		}

		d.Cells = cs
		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellsReplacedEvent); err != nil {
			return err
		}

		return c.putDashboardWithMeta(ctx, tx, d)
	})
	if err != nil {
		return &platform.Error{
			Op:  getOp(platform.OpReplaceDashboardCells),
			Err: err,
		}
	}
	return nil
}

func (c *Client) addDashboardCell(ctx context.Context, tx *bolt.Tx, id platform.ID, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	d, err := c.findDashboardByID(ctx, tx, id)
	if err != nil {
		return err
	}
	cell.ID = c.IDGenerator.ID()
	if err := c.createCellView(ctx, tx, id, cell.ID, opts.View); err != nil {
		return err
	}

	d.Cells = append(d.Cells, cell)

	if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellAddedEvent); err != nil {
		return err
	}

	return c.putDashboardWithMeta(ctx, tx, d)
}

// AddDashboardCell adds a cell to a dashboard and sets the cells ID.
func (c *Client) AddDashboardCell(ctx context.Context, id platform.ID, cell *platform.Cell, opts platform.AddDashboardCellOptions) error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		return c.addDashboardCell(ctx, tx, id, cell, opts)
	})
	if err != nil {
		return &platform.Error{
			Err: err,
			Op:  getOp(platform.OpAddDashboardCell),
		}
	}
	return nil
}

// RemoveDashboardCell removes a cell from a dashboard.
func (c *Client) RemoveDashboardCell(ctx context.Context, dashboardID, cellID platform.ID) error {
	op := getOp(platform.OpRemoveDashboardCell)
	return c.db.Update(func(tx *bolt.Tx) error {
		d, err := c.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return &platform.Error{
				Err: err,
				Op:  op,
			}
		}

		idx := -1
		for i, cell := range d.Cells {
			if cell.ID == cellID {
				idx = i
				break
			}
		}
		if idx == -1 {
			return &platform.Error{
				Code: platform.ENotFound,
				Op:   op,
				Msg:  platform.ErrCellNotFound,
			}
		}

		if err := c.deleteDashboardCellView(ctx, tx, d.ID, d.Cells[idx].ID); err != nil {
			return &platform.Error{
				Err: err,
				Op:  op,
			}
		}

		d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)

		if err := c.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellRemovedEvent); err != nil {
			return &platform.Error{
				Err: err,
				Op:  op,
			}
		}

		if err := c.putDashboardWithMeta(ctx, tx, d); err != nil {
			return &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		return nil
	})
}

// GetDashboardCellView retrieves the view for a dashboard cell.
func (c *Client) GetDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID) (*platform.View, error) {
	var v *platform.View
	err := c.db.View(func(tx *bolt.Tx) error {
		view, err := c.findDashboardCellView(ctx, tx, dashboardID, cellID)
		if err != nil {
			return err
		}

		v = view
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  getOp(platform.OpGetDashboardCellView),
		}
	}

	return v, nil
}

func (c *Client) findDashboardCellView(ctx context.Context, tx *bolt.Tx, dashboardID, cellID platform.ID) (*platform.View, error) {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return nil, platform.NewError(platform.WithErrorErr(err))
	}
	v := tx.Bucket(dashboardCellViewBucket).Get(k)
	if len(v) == 0 {
		return nil, platform.NewError(platform.WithErrorCode(platform.ENotFound), platform.WithErrorMsg(platform.ErrViewNotFound))
	}

	view := &platform.View{}
	if err := json.Unmarshal(v, view); err != nil {
		return nil, platform.NewError(platform.WithErrorErr(err))
	}

	return view, nil
}

func (c *Client) deleteDashboardCellView(ctx context.Context, tx *bolt.Tx, dashboardID, cellID platform.ID) error {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return platform.NewError(platform.WithErrorErr(err))
	}

	if err := tx.Bucket(dashboardCellViewBucket).Delete(k); err != nil {
		return platform.NewError(platform.WithErrorErr(err))
	}

	return nil
}

func (c *Client) putDashboardCellView(ctx context.Context, tx *bolt.Tx, dashboardID, cellID platform.ID, view *platform.View) error {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return platform.NewError(platform.WithErrorErr(err))
	}

	v, err := json.Marshal(view)
	if err != nil {
		return platform.NewError(platform.WithErrorErr(err))
	}

	if err := tx.Bucket(dashboardCellViewBucket).Put(k, v); err != nil {
		return platform.NewError(platform.WithErrorErr(err))
	}

	return nil
}

func encodeDashboardCellViewID(dashID, cellID platform.ID) ([]byte, error) {
	did, err := dashID.Encode()
	if err != nil {
		return nil, err
	}

	cid, err := cellID.Encode()
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	if _, err := buf.Write(did); err != nil {
		return nil, err
	}

	if _, err := buf.Write(cid); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UpdateDashboardCellView updates the view for a dashboard cell.
func (c *Client) UpdateDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	var v *platform.View

	err := c.db.Update(func(tx *bolt.Tx) error {
		view, err := c.findDashboardCellView(ctx, tx, dashboardID, cellID)
		if err != nil {
			return err
		}

		if err := upd.Apply(view); err != nil {
			return err
		}

		if err := c.putDashboardCellView(ctx, tx, dashboardID, cellID, view); err != nil {
			return err
		}

		v = view
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  getOp(platform.OpUpdateDashboardCellView),
		}
	}

	return v, nil
}

// UpdateDashboardCell udpates a cell on a dashboard.
func (c *Client) UpdateDashboardCell(ctx context.Context, dashboardID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	op := getOp(platform.OpUpdateDashboardCell)
	if err := upd.Valid(); err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
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
			return &platform.Error{
				Code: platform.ENotFound,
				Op:   op,
				Msg:  platform.ErrCellNotFound,
			}
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
		return nil, &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	return cell, nil
}

// PutDashboard will put a dashboard without setting an ID.
func (c *Client) PutDashboard(ctx context.Context, d *platform.Dashboard) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		for _, cell := range d.Cells {
			if err := c.createCellView(ctx, tx, d.ID, cell.ID, nil); err != nil {
				return err
			}
		}

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
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Op:  getOp(platform.OpUpdateDashboard),
		}
	}

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
		if pe := c.deleteDashboard(ctx, tx, id); pe != nil {
			return &platform.Error{
				Err: pe,
				Op:  getOp(platform.OpDeleteDashboard),
			}
		}
		return nil
	})
}

func (c *Client) deleteDashboard(ctx context.Context, tx *bolt.Tx, id platform.ID) *platform.Error {
	d, pe := c.findDashboardByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	for _, cell := range d.Cells {
		if err := c.deleteDashboardCellView(ctx, tx, d.ID, cell.ID); err != nil {
			return &platform.Error{
				Err: err,
			}
		}
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	if err := tx.Bucket(dashboardBucket).Delete(encodedID); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	err = c.deleteLabels(ctx, tx, platform.LabelFilter{ResourceID: id})
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	// TODO(desa): add DeleteKeyValueLog method and use it here.
	err = c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		ResourceID: id,
		Resource:   platform.DashboardsResource,
	})
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return nil
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
