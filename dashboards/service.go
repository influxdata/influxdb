package dashboards

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	influxdb "github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var (
	dashboardBucket         = []byte("dashboardsv2")
	orgDashboardIndex       = []byte("orgsdashboardsv1")
	dashboardCellViewBucket = []byte("dashboardcellviewsv1")
)

// TODO(desa): what do we want these to be?
const (
	dashboardCreatedEvent = "Dashboard Created"
	dashboardUpdatedEvent = "Dashboard Updated"
	dashboardRemovedEvent = "Dashboard Removed"

	dashboardCellsReplacedEvent = "Dashboard Cells Replaced"
	dashboardCellAddedEvent     = "Dashboard Cell Added"
	dashboardCellRemovedEvent   = "Dashboard Cell Removed"
	dashboardCellUpdatedEvent   = "Dashboard Cell Updated"
)

// OpLogStore is a type which persists and reports operation log entries on a backing
// kv store transaction.
type OpLogStore interface {
	AddLogEntryTx(ctx context.Context, tx kv.Tx, k, v []byte, t time.Time) error
	ForEachLogEntryTx(ctx context.Context, tx kv.Tx, k []byte, opts influxdb.FindOptions, fn func([]byte, time.Time) error) error
}

var _ influxdb.DashboardService = (*Service)(nil)
var _ influxdb.DashboardOperationLogService = (*Service)(nil)

type Service struct {
	kv kv.Store

	opLog OpLogStore

	IDGenerator   platform.IDGenerator
	TimeGenerator influxdb.TimeGenerator
}

// NewService constructs and configures a new dashboard service.
func NewService(store kv.Store, opLog OpLogStore) *Service {
	return &Service{
		kv:            store,
		opLog:         opLog,
		IDGenerator:   snowflake.NewIDGenerator(),
		TimeGenerator: influxdb.RealTimeGenerator{},
	}
}

// FindDashboardByID retrieves a dashboard by id.
func (s *Service) FindDashboardByID(ctx context.Context, id platform.ID) (*influxdb.Dashboard, error) {
	var d *influxdb.Dashboard

	err := s.kv.View(ctx, func(tx kv.Tx) error {
		dash, err := s.findDashboardByID(ctx, tx, id)
		if err != nil {
			return err
		}
		d = dash
		return nil
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return d, nil
}

func (s *Service) findDashboardByID(ctx context.Context, tx kv.Tx, id platform.ID) (*influxdb.Dashboard, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(dashboardBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if kv.IsNotFound(err) {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  influxdb.ErrDashboardNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	var d influxdb.Dashboard
	if err := json.Unmarshal(v, &d); err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return &d, nil
}

// FindDashboard retrieves a dashboard using an arbitrary dashboard filter.
func (s *Service) FindDashboard(ctx context.Context, filter influxdb.DashboardFilter, opts ...influxdb.FindOptions) (*influxdb.Dashboard, error) {
	if len(filter.IDs) == 1 {
		return s.FindDashboardByID(ctx, *filter.IDs[0])
	}

	var d *influxdb.Dashboard
	err := s.kv.View(ctx, func(tx kv.Tx) error {
		filterFn := filterDashboardsFn(filter)
		return s.forEachDashboard(ctx, tx, opts[0].Descending, func(dash *influxdb.Dashboard) bool {
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
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  influxdb.ErrDashboardNotFound,
		}
	}

	return d, nil
}

func filterDashboardsFn(filter influxdb.DashboardFilter) func(d *influxdb.Dashboard) bool {
	if len(filter.IDs) > 0 {
		m := map[string]struct{}{}
		for _, id := range filter.IDs {
			m[id.String()] = struct{}{}
		}
		return func(d *influxdb.Dashboard) bool {
			_, ok := m[d.ID.String()]
			return ok
		}
	}

	return func(d *influxdb.Dashboard) bool {
		return ((filter.OrganizationID == nil) || (*filter.OrganizationID == d.OrganizationID)) &&
			((filter.OwnerID == nil) || (d.OwnerID != nil && *filter.OwnerID == *d.OwnerID))
	}
}

// FindDashboards retrieves all dashboards that match an arbitrary dashboard filter.
func (s *Service) FindDashboards(ctx context.Context, filter influxdb.DashboardFilter, opts influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
	ds := []*influxdb.Dashboard{}
	if len(filter.IDs) == 1 {
		d, err := s.FindDashboardByID(ctx, *filter.IDs[0])
		if err != nil && errors.ErrorCode(err) != errors.ENotFound {
			return ds, 0, &errors.Error{
				Err: err,
			}
		}
		if d == nil {
			return ds, 0, nil
		}
		return []*influxdb.Dashboard{d}, 1, nil
	}
	err := s.kv.View(ctx, func(tx kv.Tx) error {
		dashs, err := s.findDashboards(ctx, tx, filter, opts)
		if err != nil && errors.ErrorCode(err) != errors.ENotFound {
			return err
		}
		ds = dashs
		return nil
	})

	if err != nil {
		return nil, 0, &errors.Error{
			Err: err,
		}
	}

	influxdb.SortDashboards(opts, ds)

	return ds, len(ds), nil
}

func (s *Service) findOrganizationDashboards(ctx context.Context, tx kv.Tx, orgID platform.ID, filter influxdb.DashboardFilter) ([]*influxdb.Dashboard, error) {
	idx, err := tx.Bucket(orgDashboardIndex)
	if err != nil {
		return nil, err
	}

	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	// TODO(desa): support find options.
	cur, err := idx.ForwardCursor(prefix, kv.WithCursorPrefix(prefix))
	if err != nil {
		return nil, err
	}

	ds := []*influxdb.Dashboard{}
	filterFn := filterDashboardsFn(filter)
	for k, _ := cur.Next(); k != nil; k, _ = cur.Next() {
		_, id, err := decodeOrgDashboardIndexKey(k)
		if err != nil {
			return nil, err
		}

		d, err := s.findDashboardByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}

		if filterFn(d) {
			ds = append(ds, d)
		}
	}

	return ds, nil
}

func decodeOrgDashboardIndexKey(indexKey []byte) (orgID platform.ID, dashID platform.ID, err error) {
	if len(indexKey) != 2*platform.IDLength {
		return 0, 0, &errors.Error{Code: errors.EInternal, Msg: "malformed org dashboard index key (please report this error)"}
	}

	if err := (&orgID).Decode(indexKey[:platform.IDLength]); err != nil {
		return 0, 0, &errors.Error{Code: errors.EInternal, Msg: "bad org id", Err: platform.ErrInvalidID}
	}

	if err := (&dashID).Decode(indexKey[platform.IDLength:]); err != nil {
		return 0, 0, &errors.Error{Code: errors.EInternal, Msg: "bad dashboard id", Err: platform.ErrInvalidID}
	}

	return orgID, dashID, nil
}

func (s *Service) findDashboards(ctx context.Context, tx kv.Tx, filter influxdb.DashboardFilter, opts ...influxdb.FindOptions) ([]*influxdb.Dashboard, error) {
	enforceOrgPagination := feature.EnforceOrganizationDashboardLimits().Enabled(ctx)
	if !enforceOrgPagination {
		if filter.OrganizationID != nil {
			return s.findOrganizationDashboards(ctx, tx, *filter.OrganizationID, filter)
		}
	}

	var offset, limit, count int
	var descending bool
	if len(opts) > 0 {
		offset = opts[0].Offset
		limit = opts[0].Limit
		descending = opts[0].Descending
	}

	if enforceOrgPagination {
		if filter.OrganizationID != nil {
			orgDashboards, err := s.findOrganizationDashboards(ctx, tx, *filter.OrganizationID, filter)
			if err != nil {
				return nil, &errors.Error{
					Err: err,
				}
			}
			if offset > 0 && offset < len(orgDashboards) {
				orgDashboards = orgDashboards[offset:]
			}
			if limit > 0 && limit < len(orgDashboards) {
				orgDashboards = orgDashboards[:limit]
			}
			if descending {
				for i, j := 0, len(orgDashboards)-1; i < j; i, j = i+1, j-1 {
					orgDashboards[i], orgDashboards[j] = orgDashboards[j], orgDashboards[i]
				}
			}

			return orgDashboards, nil
		}
	}

	ds := []*influxdb.Dashboard{}
	filterFn := filterDashboardsFn(filter)
	err := s.forEachDashboard(ctx, tx, descending, func(d *influxdb.Dashboard) bool {
		if filterFn(d) {
			if count >= offset {
				ds = append(ds, d)
			}
			count++
		}
		if limit > 0 && len(ds) >= limit {
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return ds, nil
}

// CreateDashboard creates a influxdb dashboard and sets d.ID.
func (s *Service) CreateDashboard(ctx context.Context, d *influxdb.Dashboard) error {
	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		d.ID = s.IDGenerator.ID()

		for _, cell := range d.Cells {
			cell.ID = s.IDGenerator.ID()

			if err := s.createCellView(ctx, tx, d.ID, cell.ID, cell.View); err != nil {
				return err
			}
		}

		if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCreatedEvent); err != nil {
			return err
		}

		if err := s.putOrganizationDashboardIndex(ctx, tx, d); err != nil {
			return err
		}

		d.Meta.CreatedAt = s.TimeGenerator.Now()
		d.Meta.UpdatedAt = s.TimeGenerator.Now()

		if err := s.putDashboardWithMeta(ctx, tx, d); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

func (s *Service) createCellView(ctx context.Context, tx kv.Tx, dashID, cellID platform.ID, view *influxdb.View) error {
	if view == nil {
		// If not view exists create the view
		view = &influxdb.View{}
	}
	// TODO: this is temporary until we can fully remove the view service.
	view.ID = cellID
	return s.putDashboardCellView(ctx, tx, dashID, cellID, view)
}

// ReplaceDashboardCells updates the positions of each cell in a dashboard concurrently.
func (s *Service) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*influxdb.Cell) error {
	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		d, err := s.findDashboardByID(ctx, tx, id)
		if err != nil {
			return err
		}

		ids := map[string]*influxdb.Cell{}
		for _, cell := range d.Cells {
			ids[cell.ID.String()] = cell
		}

		for _, cell := range cs {
			if !cell.ID.Valid() {
				return &errors.Error{
					Code: errors.EInvalid,
					Msg:  "cannot provide empty cell id",
				}
			}

			if _, ok := ids[cell.ID.String()]; !ok {
				return &errors.Error{
					Code: errors.EConflict,
					Msg:  "cannot replace cells that were not already present",
				}
			}
		}

		d.Cells = cs
		if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellsReplacedEvent); err != nil {
			return err
		}

		return s.putDashboardWithMeta(ctx, tx, d)
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

func (s *Service) addDashboardCell(ctx context.Context, tx kv.Tx, id platform.ID, cell *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
	d, err := s.findDashboardByID(ctx, tx, id)
	if err != nil {
		return err
	}
	cell.ID = s.IDGenerator.ID()
	if err := s.createCellView(ctx, tx, id, cell.ID, opts.View); err != nil {
		return err
	}

	d.Cells = append(d.Cells, cell)

	if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellAddedEvent); err != nil {
		return err
	}

	return s.putDashboardWithMeta(ctx, tx, d)
}

// AddDashboardCell adds a cell to a dashboard and sets the cells ID.
func (s *Service) AddDashboardCell(ctx context.Context, id platform.ID, cell *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		return s.addDashboardCell(ctx, tx, id, cell, opts)
	})
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}
	return nil
}

// RemoveDashboardCell removes a cell from a dashboard.
func (s *Service) RemoveDashboardCell(ctx context.Context, dashboardID, cellID platform.ID) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		d, err := s.findDashboardByID(ctx, tx, dashboardID)
		if err != nil {
			return &errors.Error{
				Err: err,
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
			return &errors.Error{
				Code: errors.ENotFound,
				Msg:  influxdb.ErrCellNotFound,
			}
		}

		if err := s.deleteDashboardCellView(ctx, tx, d.ID, d.Cells[idx].ID); err != nil {
			return &errors.Error{
				Err: err,
			}
		}

		d.Cells = append(d.Cells[:idx], d.Cells[idx+1:]...)

		if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellRemovedEvent); err != nil {
			return &errors.Error{
				Err: err,
			}
		}

		if err := s.putDashboardWithMeta(ctx, tx, d); err != nil {
			return &errors.Error{
				Err: err,
			}
		}
		return nil
	})
}

// GetDashboardCellView retrieves the view for a dashboard cell.
func (s *Service) GetDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID) (*influxdb.View, error) {
	var v *influxdb.View
	err := s.kv.View(ctx, func(tx kv.Tx) error {
		view, err := s.findDashboardCellView(ctx, tx, dashboardID, cellID)
		if err != nil {
			return err
		}

		v = view
		return nil
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return v, nil
}

func (s *Service) findDashboardCellView(ctx context.Context, tx kv.Tx, dashboardID, cellID platform.ID) (*influxdb.View, error) {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return nil, errors.NewError(errors.WithErrorErr(err))
	}

	vb, err := tx.Bucket(dashboardCellViewBucket)
	if err != nil {
		return nil, err
	}

	v, err := vb.Get(k)
	if kv.IsNotFound(err) {
		return nil, errors.NewError(errors.WithErrorCode(errors.ENotFound), errors.WithErrorMsg(influxdb.ErrViewNotFound))
	}

	if err != nil {
		return nil, err
	}

	view := &influxdb.View{}
	if err := json.Unmarshal(v, view); err != nil {
		return nil, errors.NewError(errors.WithErrorErr(err))
	}

	return view, nil
}

func (s *Service) deleteDashboardCellView(ctx context.Context, tx kv.Tx, dashboardID, cellID platform.ID) error {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return errors.NewError(errors.WithErrorErr(err))
	}

	vb, err := tx.Bucket(dashboardCellViewBucket)
	if err != nil {
		return err
	}

	if err := vb.Delete(k); err != nil {
		return errors.NewError(errors.WithErrorErr(err))
	}

	return nil
}

func (s *Service) putDashboardCellView(ctx context.Context, tx kv.Tx, dashboardID, cellID platform.ID, view *influxdb.View) error {
	k, err := encodeDashboardCellViewID(dashboardID, cellID)
	if err != nil {
		return errors.NewError(errors.WithErrorErr(err))
	}

	v, err := json.Marshal(view)
	if err != nil {
		return errors.NewError(errors.WithErrorErr(err))
	}

	vb, err := tx.Bucket(dashboardCellViewBucket)
	if err != nil {
		return err
	}

	if err := vb.Put(k, v); err != nil {
		return errors.NewError(errors.WithErrorErr(err))
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
func (s *Service) UpdateDashboardCellView(ctx context.Context, dashboardID, cellID platform.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
	var v *influxdb.View

	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		view, err := s.findDashboardCellView(ctx, tx, dashboardID, cellID)
		if err != nil {
			return err
		}

		if err := upd.Apply(view); err != nil {
			return err
		}

		if err := s.putDashboardCellView(ctx, tx, dashboardID, cellID, view); err != nil {
			return err
		}

		v = view
		return nil
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return v, nil
}

// UpdateDashboardCell udpates a cell on a dashboard.
func (s *Service) UpdateDashboardCell(ctx context.Context, dashboardID, cellID platform.ID, upd influxdb.CellUpdate) (*influxdb.Cell, error) {
	if err := upd.Valid(); err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	var cell *influxdb.Cell
	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		d, err := s.findDashboardByID(ctx, tx, dashboardID)
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
			return &errors.Error{
				Code: errors.ENotFound,
				Msg:  influxdb.ErrCellNotFound,
			}
		}

		if err := upd.Apply(d.Cells[idx]); err != nil {
			return err
		}

		cell = d.Cells[idx]

		if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardCellUpdatedEvent); err != nil {
			return err
		}

		return s.putDashboardWithMeta(ctx, tx, d)
	})

	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return cell, nil
}

// PutDashboard will put a dashboard without setting an ID.
func (s *Service) PutDashboard(ctx context.Context, d *influxdb.Dashboard) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		for _, cell := range d.Cells {
			if err := s.createCellView(ctx, tx, d.ID, cell.ID, cell.View); err != nil {
				return err
			}
		}

		if err := s.putOrganizationDashboardIndex(ctx, tx, d); err != nil {
			return err
		}

		return s.putDashboard(ctx, tx, d)
	})
}

func encodeOrgDashboardIndex(orgID platform.ID, dashID platform.ID) ([]byte, error) {
	oid, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	did, err := dashID.Encode()
	if err != nil {
		return nil, err
	}

	key := make([]byte, 0, len(oid)+len(did))
	key = append(key, oid...)
	key = append(key, did...)

	return key, nil
}

func (s *Service) putOrganizationDashboardIndex(ctx context.Context, tx kv.Tx, d *influxdb.Dashboard) error {
	k, err := encodeOrgDashboardIndex(d.OrganizationID, d.ID)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(orgDashboardIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(k, nil); err != nil {
		return err
	}

	return nil
}

func (s *Service) removeOrganizationDashboardIndex(ctx context.Context, tx kv.Tx, d *influxdb.Dashboard) error {
	k, err := encodeOrgDashboardIndex(d.OrganizationID, d.ID)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(orgDashboardIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(k); err != nil {
		return err
	}

	return nil
}

func (s *Service) putDashboard(ctx context.Context, tx kv.Tx, d *influxdb.Dashboard) error {
	v, err := json.Marshal(d)
	if err != nil {
		return err
	}

	encodedID, err := d.ID.Encode()
	if err != nil {
		return err
	}

	b, err := tx.Bucket(dashboardBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return err
	}

	return nil
}

func (s *Service) putDashboardWithMeta(ctx context.Context, tx kv.Tx, d *influxdb.Dashboard) error {
	// TODO(desa): don't populate this here. use the first/last methods of the oplog to get meta fields.
	d.Meta.UpdatedAt = s.TimeGenerator.Now()
	return s.putDashboard(ctx, tx, d)
}

// forEachDashboard will iterate through all dashboards while fn returns true.
func (s *Service) forEachDashboard(ctx context.Context, tx kv.Tx, descending bool, fn func(*influxdb.Dashboard) bool) error {
	b, err := tx.Bucket(dashboardBucket)
	if err != nil {
		return err
	}

	direction := kv.CursorAscending
	if descending {
		direction = kv.CursorDescending
	}

	cur, err := b.ForwardCursor(nil, kv.WithCursorDirection(direction))
	if err != nil {
		return err
	}

	for k, v := cur.Next(); k != nil; k, v = cur.Next() {
		d := &influxdb.Dashboard{}
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
func (s *Service) UpdateDashboard(ctx context.Context, id platform.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
	if err := upd.Valid(); err != nil {
		return nil, err
	}

	var d *influxdb.Dashboard
	err := s.kv.Update(ctx, func(tx kv.Tx) error {
		dash, err := s.updateDashboard(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		d = dash

		return nil
	})
	if err != nil {
		return nil, &errors.Error{
			Err: err,
		}
	}

	return d, err
}

func (s *Service) updateDashboard(ctx context.Context, tx kv.Tx, id platform.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
	d, err := s.findDashboardByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Cells != nil {
		for _, c := range *upd.Cells {
			if !c.ID.Valid() {
				c.ID = s.IDGenerator.ID()
				if c.View != nil {
					c.View.ViewContents.ID = c.ID
				}
			}
		}
		for _, c := range d.Cells {
			if err := s.deleteDashboardCellView(ctx, tx, d.ID, c.ID); err != nil {
				return nil, err
			}
		}
	}

	if err := upd.Apply(d); err != nil {
		return nil, err
	}

	if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardUpdatedEvent); err != nil {
		return nil, err
	}

	if err := s.putDashboardWithMeta(ctx, tx, d); err != nil {
		return nil, err
	}

	if upd.Cells != nil {
		for _, c := range d.Cells {
			if err := s.putDashboardCellView(ctx, tx, d.ID, c.ID, c.View); err != nil {
				return nil, err
			}
		}
	}

	return d, nil
}

// DeleteDashboard deletes a dashboard and prunes it from the index.
func (s *Service) DeleteDashboard(ctx context.Context, id platform.ID) error {
	return s.kv.Update(ctx, func(tx kv.Tx) error {
		if pe := s.deleteDashboard(ctx, tx, id); pe != nil {
			return &errors.Error{
				Err: pe,
			}
		}
		return nil
	})
}

func (s *Service) deleteDashboard(ctx context.Context, tx kv.Tx, id platform.ID) error {
	d, err := s.findDashboardByID(ctx, tx, id)
	if err != nil {
		return err
	}

	for _, cell := range d.Cells {
		if err := s.deleteDashboardCellView(ctx, tx, d.ID, cell.ID); err != nil {
			return &errors.Error{
				Err: err,
			}
		}
	}

	encodedID, err := id.Encode()
	if err != nil {
		return &errors.Error{
			Err: err,
		}
	}

	if err := s.removeOrganizationDashboardIndex(ctx, tx, d); err != nil {
		return errors.NewError(errors.WithErrorErr(err))
	}

	b, err := tx.Bucket(dashboardBucket)
	if err != nil {
		return err
	}

	if err := b.Delete(encodedID); err != nil {
		return &errors.Error{
			Err: err,
		}
	}

	if err := s.appendDashboardEventToLog(ctx, tx, d.ID, dashboardRemovedEvent); err != nil {
		return &errors.Error{
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
func (s *Service) GetDashboardOperationLog(ctx context.Context, id platform.ID, opts influxdb.FindOptions) ([]*influxdb.OperationLogEntry, int, error) {
	// TODO(desa): might be worthwhile to allocate a slice of size opts.Limit
	log := []*influxdb.OperationLogEntry{}

	err := s.kv.View(ctx, func(tx kv.Tx) error {
		key, err := encodeDashboardOperationLogKey(id)
		if err != nil {
			return err
		}

		return s.opLog.ForEachLogEntryTx(ctx, tx, key, opts, func(v []byte, t time.Time) error {
			e := &influxdb.OperationLogEntry{}
			if err := json.Unmarshal(v, e); err != nil {
				return err
			}
			e.Time = t

			log = append(log, e)

			return nil
		})
	})

	if err != nil && err != kv.ErrKeyValueLogBoundsNotFound {
		return nil, 0, err
	}

	return log, len(log), nil
}

func (s *Service) appendDashboardEventToLog(ctx context.Context, tx kv.Tx, id platform.ID, st string) error {
	e := &influxdb.OperationLogEntry{
		Description: st,
	}
	// TODO(desa): this is fragile and non explicit since it requires an authorizer to be on context. It should be
	//             replaced with a higher level transaction so that adding to the log can take place in the http handler
	//             where the userID will exist explicitly.

	a, err := icontext.GetAuthorizer(ctx)
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

	return s.opLog.AddLogEntryTx(ctx, tx, k, v, s.TimeGenerator.Now())
}
