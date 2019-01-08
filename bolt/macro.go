package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	macroBucket = []byte("macros")
)

func (c *Client) initializeMacros(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(macroBucket)); err != nil {
		return err
	}
	return nil
}

// FindMacros returns all macros in the store
func (c *Client) FindMacros(ctx context.Context) ([]*platform.Macro, error) {
	op := getOp(platform.OpFindMacros)
	macros := []*platform.Macro{}
	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(macroBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			macro := platform.Macro{}

			err := json.Unmarshal(v, &macro)
			if err != nil {
				return &platform.Error{
					Err: err,
					Op:  op,
				}
			}

			macros = append(macros, &macro)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return macros, nil
}

// FindMacroByID finds a single macro in the store by its ID
func (c *Client) FindMacroByID(ctx context.Context, id platform.ID) (*platform.Macro, error) {
	op := getOp(platform.OpFindMacroByID)
	var macro *platform.Macro
	err := c.db.View(func(tx *bolt.Tx) error {
		m, pe := c.findMacroByID(ctx, tx, id)
		if pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}
		macro = m
		return nil
	})
	if err != nil {
		return nil, err
	}

	return macro, nil
}

func (c *Client) findMacroByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Macro, *platform.Error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	d := tx.Bucket(macroBucket).Get(encID)
	if d == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "macro not found",
		}
	}

	macro := &platform.Macro{}
	err = json.Unmarshal(d, &macro)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return macro, nil
}

// CreateMacro creates a new macro and assigns it an ID
func (c *Client) CreateMacro(ctx context.Context, macro *platform.Macro) error {
	op := getOp(platform.OpCreateMacro)
	return c.db.Update(func(tx *bolt.Tx) error {
		macro.ID = c.IDGenerator.ID()
		if pe := c.putMacro(ctx, tx, macro); pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}

		}
		return nil
	})
}

// ReplaceMacro puts a macro in the store
func (c *Client) ReplaceMacro(ctx context.Context, macro *platform.Macro) error {
	op := getOp(platform.OpReplaceMacro)
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.putMacro(ctx, tx, macro); pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}
		return nil
	})
}

func (c *Client) putMacro(ctx context.Context, tx *bolt.Tx, macro *platform.Macro) *platform.Error {
	j, err := json.Marshal(macro)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	encID, err := macro.ID.Encode()
	if err != nil {
		return &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	if err := tx.Bucket(macroBucket).Put(encID, j); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

// UpdateMacro updates a single macro in the store with a changeset
func (c *Client) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	op := getOp(platform.OpUpdateMacro)
	var macro *platform.Macro
	err := c.db.Update(func(tx *bolt.Tx) error {
		m, pe := c.findMacroByID(ctx, tx, id)
		if pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}

		if err := update.Apply(m); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		macro = m
		if pe = c.putMacro(ctx, tx, macro); pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}
		return nil
	})

	return macro, err
}

// DeleteMacro removes a single macro from the store by its ID
func (c *Client) DeleteMacro(ctx context.Context, id platform.ID) error {
	op := getOp(platform.OpDeleteMacro)
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(macroBucket)

		encID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Code: platform.EInvalid,
				Op:   op,
				Err:  err,
			}
		}

		d := b.Get(encID)
		if d == nil {
			return &platform.Error{
				Code: platform.ENotFound,
				Op:   op,
				Msg:  "macro not found",
			}
		}

		if err := b.Delete(encID); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		return nil
	})
}
