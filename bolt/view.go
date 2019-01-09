package bolt

import (
	"context"
	"encoding/json"
	"sync"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
)

var (
	viewBucket = []byte("viewsv2")
)

func (c *Client) initializeViews(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(viewBucket)); err != nil {
		return err
	}
	return nil
}

// FindViewByID retrieves a view by id.
func (c *Client) FindViewByID(ctx context.Context, id platform.ID) (*platform.View, error) {
	var d *platform.View

	err := c.db.View(func(tx *bolt.Tx) error {
		dash, err := c.findViewByID(ctx, tx, id)
		if err != nil {
			return &platform.Error{
				Err: err,
				Op:  getOp(platform.OpFindViewByID),
			}
		}
		d = dash
		return nil
	})

	return d, err
}

func (c *Client) findViewByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.View, *platform.Error) {
	var d platform.View

	encodedID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	v := tx.Bucket(viewBucket).Get(encodedID)
	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrViewNotFound,
		}
	}

	if err := json.Unmarshal(v, &d); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return &d, nil
}

// FindView retrieves a view using an arbitrary view filter.
func (c *Client) FindView(ctx context.Context, filter platform.ViewFilter) (*platform.View, error) {
	if filter.ID != nil {
		return c.FindViewByID(ctx, *filter.ID)
	}

	var d *platform.View
	err := c.db.View(func(tx *bolt.Tx) error {
		filterFn := filterViewsFn(filter)
		return c.forEachView(ctx, tx, func(dash *platform.View) bool {
			if filterFn(dash) {
				d = dash
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	if d == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrViewNotFound,
		}
	}

	return d, nil
}

func filterViewsFn(filter platform.ViewFilter) func(v *platform.View) bool {
	if filter.ID != nil {
		return func(v *platform.View) bool {
			return v.ID == *filter.ID
		}
	}

	if len(filter.Types) > 0 {
		var sm sync.Map
		for _, t := range filter.Types {
			sm.Store(t, true)
		}
		return func(v *platform.View) bool {
			_, ok := sm.Load(v.Properties.GetType())
			return ok
		}
	}

	return func(v *platform.View) bool { return true }
}

// FindViews retrives all views that match an arbitrary view filter.
func (c *Client) FindViews(ctx context.Context, filter platform.ViewFilter) ([]*platform.View, int, error) {
	ds := []*platform.View{}
	op := getOp(platform.OpFindViews)
	if filter.ID != nil {
		d, err := c.FindViewByID(ctx, *filter.ID)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		if d != nil {
			ds = append(ds, d)
		}

		return ds, 1, nil
	}

	err := c.db.View(func(tx *bolt.Tx) error {
		dashs, err := c.findViews(ctx, tx, filter)
		if err != nil {
			return &platform.Error{
				Err: err,
				Op:  op,
			}
		}
		ds = dashs
		return nil
	})

	return ds, len(ds), err
}

func (c *Client) findViews(ctx context.Context, tx *bolt.Tx, filter platform.ViewFilter) ([]*platform.View, error) {
	ds := []*platform.View{}

	filterFn := filterViewsFn(filter)
	err := c.forEachView(ctx, tx, func(d *platform.View) bool {
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

// CreateView creates a platform view and sets d.ID.
func (c *Client) CreateView(ctx context.Context, d *platform.View) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.createView(ctx, tx, d); pe != nil {
			return &platform.Error{
				Op:  getOp(platform.OpCreateView),
				Err: pe,
			}
		}
		return nil
	})
}

func (c *Client) createView(ctx context.Context, tx *bolt.Tx, d *platform.View) *platform.Error {
	d.ID = c.IDGenerator.ID()
	return c.putView(ctx, tx, d)
}

// PutView will put a view without setting an ID.
func (c *Client) PutView(ctx context.Context, d *platform.View) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.putView(ctx, tx, d); pe != nil {
			return pe
		}
		return nil
	})
}

func (c *Client) putView(ctx context.Context, tx *bolt.Tx, d *platform.View) *platform.Error {
	v, err := json.Marshal(d)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	encodedID, err := d.ID.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	if err := tx.Bucket(viewBucket).Put(encodedID, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return nil
}

// forEachView will iterate through all views while fn returns true.
func (c *Client) forEachView(ctx context.Context, tx *bolt.Tx, fn func(*platform.View) bool) error {
	cur := tx.Bucket(viewBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		d := &platform.View{}
		if err := json.Unmarshal(v, d); err != nil {
			return err
		}
		if !fn(d) {
			break
		}
	}

	return nil
}

// UpdateView updates a view according the parameters set on upd.
func (c *Client) UpdateView(ctx context.Context, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	var d *platform.View
	err := c.db.Update(func(tx *bolt.Tx) error {
		dash, pe := c.updateView(ctx, tx, id, upd)
		if pe != nil {
			return &platform.Error{
				Err: pe,
				Op:  getOp(platform.OpUpdateView),
			}
		}
		d = dash
		return nil
	})

	return d, err
}

func (c *Client) updateView(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.ViewUpdate) (*platform.View, error) {
	d, err := c.findViewByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		d.Name = *upd.Name
	}

	if upd.Properties != nil {
		d.Properties = upd.Properties
	}

	if err := c.putView(ctx, tx, d); err != nil {
		return nil, err
	}

	return d, nil
}

// DeleteView deletes a view and prunes it from the index.
func (c *Client) DeleteView(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.deleteView(ctx, tx, id); pe != nil {
			return &platform.Error{
				Err: pe,
				Op:  getOp(platform.OpDeleteView),
			}
		}
		return nil
	})
}

func (c *Client) deleteView(ctx context.Context, tx *bolt.Tx, id platform.ID) *platform.Error {
	_, pe := c.findViewByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	if err := tx.Bucket(viewBucket).Delete(encodedID); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	if err := c.deleteUserResourceMappings(ctx, tx, platform.UserResourceMappingFilter{
		ResourceID: id,
		Resource:   platform.DashboardsResource,
	}); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}
