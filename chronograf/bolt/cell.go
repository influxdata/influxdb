package bolt

import (
	"context"
	"encoding/json"
	"strconv"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform/chronograf/v2"
)

var (
	cellBucket = []byte("cellsv2")
)

func (c *Client) initializeCells(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(cellBucket)); err != nil {
		return err
	}
	return nil
}

// FindCellByID retrieves a cell by id.
func (c *Client) FindCellByID(ctx context.Context, id platform.ID) (*platform.Cell, error) {
	var d *platform.Cell

	err := c.db.View(func(tx *bolt.Tx) error {
		dash, err := c.findCellByID(ctx, tx, id)
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

func (c *Client) findCellByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Cell, error) {
	var d platform.Cell

	v := tx.Bucket(cellBucket).Get([]byte(id))

	if len(v) == 0 {
		return nil, platform.ErrCellNotFound
	}

	if err := json.Unmarshal(v, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

// FindCell retrieves a cell using an arbitrary cell filter.
func (c *Client) FindCell(ctx context.Context, filter platform.CellFilter) (*platform.Cell, error) {
	if filter.ID != nil {
		return c.FindCellByID(ctx, *filter.ID)
	}

	var d *platform.Cell
	err := c.db.View(func(tx *bolt.Tx) error {
		filterFn := filterCellsFn(filter)
		return c.forEachCell(ctx, tx, func(dash *platform.Cell) bool {
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
		return nil, platform.ErrCellNotFound
	}

	return d, nil
}

func filterCellsFn(filter platform.CellFilter) func(d *platform.Cell) bool {
	if filter.ID != nil {
		return func(d *platform.Cell) bool {
			return d.ID == *filter.ID
		}
	}

	return func(d *platform.Cell) bool { return true }
}

// FindCells retrives all cells that match an arbitrary cell filter.
func (c *Client) FindCells(ctx context.Context, filter platform.CellFilter) ([]*platform.Cell, int, error) {
	if filter.ID != nil {
		d, err := c.FindCellByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.Cell{d}, 1, nil
	}

	ds := []*platform.Cell{}
	err := c.db.View(func(tx *bolt.Tx) error {
		dashs, err := c.findCells(ctx, tx, filter)
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

func (c *Client) findCells(ctx context.Context, tx *bolt.Tx, filter platform.CellFilter) ([]*platform.Cell, error) {
	ds := []*platform.Cell{}

	filterFn := filterCellsFn(filter)
	err := c.forEachCell(ctx, tx, func(d *platform.Cell) bool {
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

// CreateCell creates a platform cell and sets d.ID.
func (c *Client) CreateCell(ctx context.Context, d *platform.Cell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		id, err := tx.Bucket(cellBucket).NextSequence()
		if err != nil {
			return err
		}
		d.ID = platform.ID(strconv.Itoa(int(id)))

		return c.putCell(ctx, tx, d)
	})
}

// PutCell will put a cell without setting an ID.
func (c *Client) PutCell(ctx context.Context, d *platform.Cell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putCell(ctx, tx, d)
	})
}

func (c *Client) putCell(ctx context.Context, tx *bolt.Tx, d *platform.Cell) error {
	v, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := tx.Bucket(cellBucket).Put([]byte(d.ID), v); err != nil {
		return err
	}
	return nil
}

// forEachCell will iterate through all cells while fn returns true.
func (c *Client) forEachCell(ctx context.Context, tx *bolt.Tx, fn func(*platform.Cell) bool) error {
	cur := tx.Bucket(cellBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		d := &platform.Cell{}
		if err := json.Unmarshal(v, d); err != nil {
			return err
		}
		if !fn(d) {
			break
		}
	}

	return nil
}

// UpdateCell updates a cell according the parameters set on upd.
func (c *Client) UpdateCell(ctx context.Context, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	var d *platform.Cell
	err := c.db.Update(func(tx *bolt.Tx) error {
		dash, err := c.updateCell(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		d = dash
		return nil
	})

	return d, err
}

func (c *Client) updateCell(ctx context.Context, tx *bolt.Tx, id platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	d, err := c.findCellByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		d.Name = *upd.Name
	}

	if upd.Visualization != nil {
		d.Visualization = upd.Visualization
	}

	if err := c.putCell(ctx, tx, d); err != nil {
		return nil, err
	}

	return d, nil
}

// DeleteCell deletes a cell and prunes it from the index.
func (c *Client) DeleteCell(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.deleteCell(ctx, tx, id)
	})
}

func (c *Client) deleteCell(ctx context.Context, tx *bolt.Tx, id platform.ID) error {
	_, err := c.findCellByID(ctx, tx, id)
	if err != nil {
		return err
	}
	return tx.Bucket(cellBucket).Delete([]byte(id))
}
