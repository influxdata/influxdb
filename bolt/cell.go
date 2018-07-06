package bolt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
	chronograf "github.com/influxdata/chronograf/v2"
	"github.com/influxdata/platform"
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

//func (c *Client) setOrganizationOnCell(ctx context.Context, tx *bolt.Tx, d *chronograf.Cell) error {
//	o, err := c.findOrganizationByID(ctx, tx, d.OrganizationID)
//	if err != nil {
//		return err
//	}
//	d.Organization = o.Name
//	return nil
//}

// FindCellByID retrieves a cell by id.
func (c *Client) FindCellByID(ctx context.Context, id platform.ID) (*chronograf.Cell, error) {
	var d *chronograf.Cell

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

func (c *Client) findCellByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*chronograf.Cell, error) {
	var d chronograf.Cell

	v := tx.Bucket(cellBucket).Get(id)

	if len(v) == 0 {
		// TODO: Make standard error
		return nil, fmt.Errorf("cell not found")
	}

	if err := json.Unmarshal(v, &d); err != nil {
		return nil, err
	}

	//if err := c.setOrganizationOnCell(ctx, tx, &d); err != nil {
	//	return nil, err
	//}

	return &d, nil
}

// FindCell retrieves a cell using an arbitrary cell filter.
// Filters using ID, or OrganizationID and cell Name should be efficient.
// Other filters will do a linear scan across cells until it finds a match.
func (c *Client) FindCell(ctx context.Context, filter chronograf.CellFilter) (*chronograf.Cell, error) {
	if filter.ID != nil {
		return c.FindCellByID(ctx, *filter.ID)
	}

	var d *chronograf.Cell
	err := c.db.View(func(tx *bolt.Tx) error {
		//if filter.Organization != nil {
		//	o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
		//	if err != nil {
		//		return err
		//	}
		//	filter.OrganizationID = &o.ID
		//}

		filterFn := filterCellsFn(filter)
		return c.forEachCell(ctx, tx, func(dash *chronograf.Cell) bool {
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
		return nil, fmt.Errorf("cell not found")
	}

	return d, nil
}

func filterCellsFn(filter chronograf.CellFilter) func(d *chronograf.Cell) bool {
	if filter.ID != nil {
		return func(d *chronograf.Cell) bool {
			return bytes.Equal(d.ID, *filter.ID)
		}
	}

	//if filter.OrganizationID != nil {
	//	return func(d *chronograf.Cell) bool {
	//		return bytes.Equal(d.OrganizationID, *filter.OrganizationID)
	//	}
	//}

	return func(d *chronograf.Cell) bool { return true }
}

// FindCells retrives all cells that match an arbitrary cell filter.
// Filters using ID, or OrganizationID and cell Name should be efficient.
// Other filters will do a linear scan across all cells searching for a match.
func (c *Client) FindCells(ctx context.Context, filter chronograf.CellFilter) ([]*chronograf.Cell, int, error) {
	if filter.ID != nil {
		d, err := c.FindCellByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*chronograf.Cell{d}, 1, nil
	}

	ds := []*chronograf.Cell{}
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

func (c *Client) findCells(ctx context.Context, tx *bolt.Tx, filter chronograf.CellFilter) ([]*chronograf.Cell, error) {
	ds := []*chronograf.Cell{}
	//if filter.Organization != nil {
	//	o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
	//	if err != nil {
	//		return nil, err
	//	}
	//	filter.OrganizationID = &o.ID
	//}

	filterFn := filterCellsFn(filter)
	err := c.forEachCell(ctx, tx, func(d *chronograf.Cell) bool {
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
func (c *Client) CreateCell(ctx context.Context, d *chronograf.Cell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		//if len(d.OrganizationID) == 0 {
		//	o, err := c.findOrganizationByName(ctx, tx, d.Organization)
		//	if err != nil {
		//		return err
		//	}
		//	d.OrganizationID = o.ID
		//}

		id, err := tx.Bucket(cellBucket).NextSequence()
		if err != nil {
			return err
		}
		d.ID = platform.ID(fmt.Sprintf("%d", id))

		return c.putCell(ctx, tx, d)
	})
}

// PutCell will put a cell without setting an ID.
func (c *Client) PutCell(ctx context.Context, d *chronograf.Cell) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putCell(ctx, tx, d)
	})
}

func (c *Client) putCell(ctx context.Context, tx *bolt.Tx, d *chronograf.Cell) error {
	//d.Organization = ""
	v, err := json.Marshal(d)
	if err != nil {
		return err
	}
	if err := tx.Bucket(cellBucket).Put(d.ID, v); err != nil {
		return err
	}
	//return c.setOrganizationOnCell(ctx, tx, d)
	return nil
}

// forEachCell will iterate through all cells while fn returns true.
func (c *Client) forEachCell(ctx context.Context, tx *bolt.Tx, fn func(*chronograf.Cell) bool) error {
	cur := tx.Bucket(cellBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		d := &chronograf.Cell{}
		if err := json.Unmarshal(v, d); err != nil {
			return err
		}
		//if err := c.setOrganizationOnCell(ctx, tx, d); err != nil {
		//	return err
		//}
		if !fn(d) {
			break
		}
	}

	return nil
}

// UpdateCell updates a cell according the parameters set on upd.
func (c *Client) UpdateCell(ctx context.Context, id platform.ID, upd chronograf.CellUpdate) (*chronograf.Cell, error) {
	var d *chronograf.Cell
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

func (c *Client) updateCell(ctx context.Context, tx *bolt.Tx, id platform.ID, upd chronograf.CellUpdate) (*chronograf.Cell, error) {
	d, err := c.findCellByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		d.Name = *upd.Name
	}

	if err := c.putCell(ctx, tx, d); err != nil {
		return nil, err
	}

	//if err := c.setOrganizationOnCell(ctx, tx, d); err != nil {
	//	return nil, err
	//}

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
	return tx.Bucket(cellBucket).Delete(id)
}
