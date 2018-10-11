package bolt

import (
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
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
	macros := []*platform.Macro{}

	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(macroBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			macro := platform.Macro{}

			err := json.Unmarshal(v, &macro)
			if err != nil {
				return err
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
	var macro *platform.Macro
	err := c.db.View(func(tx *bolt.Tx) error {
		m, err := c.findMacroByID(ctx, tx, id)
		if err != nil {
			return err
		}
		macro = m
		return nil
	})
	if err != nil {
		return nil, err
	}

	return macro, nil
}

func (c *Client) findMacroByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Macro, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	d := tx.Bucket(macroBucket).Get(encID)
	if d == nil {
		return nil, kerrors.Errorf(kerrors.NotFound, "macro with ID %v not found", id)
	}

	macro := &platform.Macro{}
	err = json.Unmarshal(d, &macro)
	if err != nil {
		return nil, err
	}

	return macro, nil
}

// CreateMacro creates a new macro and assigns it an ID
func (c *Client) CreateMacro(ctx context.Context, macro *platform.Macro) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		macro.ID = c.IDGenerator.ID()

		return c.putMacro(ctx, tx, macro)
	})
}

// ReplaceMacro puts a macro in the store
func (c *Client) ReplaceMacro(ctx context.Context, macro *platform.Macro) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.putMacro(ctx, tx, macro)
	})
}

func (c *Client) putMacro(ctx context.Context, tx *bolt.Tx, macro *platform.Macro) error {
	j, err := json.Marshal(macro)
	if err != nil {
		return err
	}
	encID, err := macro.ID.Encode()
	if err != nil {
		return err
	}

	return tx.Bucket(macroBucket).Put(encID, j)
}

// UpdateMacro updates a single macro in the store with a changeset
func (c *Client) UpdateMacro(ctx context.Context, id platform.ID, update *platform.MacroUpdate) (*platform.Macro, error) {
	var macro *platform.Macro

	err := c.db.Update(func(tx *bolt.Tx) error {
		m, err := c.findMacroByID(ctx, tx, id)
		if err != nil {
			return err
		}

		if err = update.Apply(m); err != nil {
			return err
		}

		macro = m

		return c.putMacro(ctx, tx, macro)
	})
	if err != nil {
		return nil, err
	}

	return macro, nil
}

// DeleteMacro removes a single macro from the store by its ID
func (c *Client) DeleteMacro(ctx context.Context, id platform.ID) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(macroBucket)

		encID, err := id.Encode()
		if err != nil {
			return err
		}

		d := b.Get(encID)
		if d == nil {
			return kerrors.Errorf(kerrors.NotFound, "macro with ID %v not found", id)
		}

		b.Delete(encID)

		return nil
	})
}
