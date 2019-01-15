package bolt

import (
	"bytes"
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	macroBucket    = []byte("macrosv1")
	macroOrgsIndex = []byte("macroorgsv1")
)

func (c *Client) initializeMacros(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(macroBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(macroOrgsIndex); err != nil {
		return err
	}
	return nil
}

func decodeMacroOrgsIndexKey(indexKey []byte) (orgID platform.ID, macroID platform.ID, err error) {
	if len(indexKey) != 2*platform.IDLength {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "malformed macro orgs index key (please report this error)",
		}
	}

	if err := (&orgID).Decode(indexKey[:platform.IDLength]); err != nil {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "bad org id",
			Err:  platform.ErrInvalidID,
		}
	}

	if err := (&macroID).Decode(indexKey[platform.IDLength:]); err != nil {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "bad macro id",
			Err:  platform.ErrInvalidID,
		}
	}

	return orgID, macroID, nil
}

func (c *Client) findOrganizationMacros(ctx context.Context, tx *bolt.Tx, orgID platform.ID) ([]*platform.Macro, error) {
	// TODO(leodido): support find options
	cur := tx.Bucket(macroOrgsIndex).Cursor()
	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	macros := []*platform.Macro{}
	for k, _ := cur.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		_, id, err := decodeMacroOrgsIndexKey(k)
		if err != nil {
			return nil, err
		}

		m, err := c.findMacroByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}

		macros = append(macros, m)
	}

	return macros, nil
}

func (c *Client) findMacros(ctx context.Context, tx *bolt.Tx, filter platform.MacroFilter) ([]*platform.Macro, error) {
	if filter.OrganizationID != nil {
		return c.findOrganizationMacros(ctx, tx, *filter.OrganizationID)
	}

	if filter.Organization != nil {
		o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		return c.findOrganizationMacros(ctx, tx, o.ID)
	}

	macros := []*platform.Macro{}
	filterFn := filterMacrosFn(filter)
	err := c.forEachMacro(ctx, tx, func(m *platform.Macro) bool {
		if filterFn(m) {
			macros = append(macros, m)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return macros, nil
}

func filterMacrosFn(filter platform.MacroFilter) func(m *platform.Macro) bool {
	if filter.ID != nil {
		return func(m *platform.Macro) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *platform.Macro) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *platform.Macro) bool { return true }
}

// forEachMacro will iterate through all macros while fn returns true.
func (c *Client) forEachMacro(ctx context.Context, tx *bolt.Tx, fn func(*platform.Macro) bool) error {
	cur := tx.Bucket(macroBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &platform.Macro{}
		if err := json.Unmarshal(v, m); err != nil {
			return err
		}
		if !fn(m) {
			break
		}
	}

	return nil
}

// FindMacros returns all macros in the store
func (c *Client) FindMacros(ctx context.Context, filter platform.MacroFilter, opt ...platform.FindOptions) ([]*platform.Macro, error) {
	// todo(leodido) > handle find options
	op := getOp(platform.OpFindMacros)
	res := []*platform.Macro{}
	err := c.db.View(func(tx *bolt.Tx) error {
		macros, err := c.findMacros(ctx, tx, filter)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return err
		}
		res = macros
		return nil
	})

	if err != nil {
		return nil, &platform.Error{
			Op:  op,
			Err: err,
		}
	}

	return res, nil
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

func (c *Client) findMacroByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Macro, error) {
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
			Msg:  platform.ErrMacroNotFound,
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

		if err := c.putMacroOrgsIndex(ctx, tx, macro); err != nil {
			return err
		}

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
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.putMacroOrgsIndex(ctx, tx, macro); err != nil {
			return err
		}
		return c.putMacro(ctx, tx, macro)
	})
}

func encodeMacroOrgsIndex(macro *platform.Macro) ([]byte, error) {
	oID, err := macro.OrganizationID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Msg: "bad organization id",
		}
	}

	mID, err := macro.ID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Msg: "bad macro id",
		}
	}

	key := make([]byte, 0, platform.IDLength*2)
	key = append(key, oID...)
	key = append(key, mID...)

	return key, nil
}

func (c *Client) putMacroOrgsIndex(ctx context.Context, tx *bolt.Tx, macro *platform.Macro) error {
	key, err := encodeMacroOrgsIndex(macro)
	if err != nil {
		return err
	}

	if err := tx.Bucket(macroOrgsIndex).Put(key, nil); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

func (c *Client) removeMacroOrgsIndex(ctx context.Context, tx *bolt.Tx, macro *platform.Macro) error {
	key, err := encodeMacroOrgsIndex(macro)
	if err != nil {
		return err
	}

	if err := tx.Bucket(macroOrgsIndex).Delete(key); err != nil {
		return err
	}

	return nil
}

func (c *Client) putMacro(ctx context.Context, tx *bolt.Tx, macro *platform.Macro) error {
	m, err := json.Marshal(macro)
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

	if err := tx.Bucket(macroBucket).Put(encID, m); err != nil {
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
		m, pe := c.findMacroByID(ctx, tx, id)
		if pe != nil {
			return pe
		}

		encID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Err: err,
			}
		}

		if err := c.removeMacroOrgsIndex(ctx, tx, m); err != nil {
			return platform.NewError(platform.WithErrorErr(err))
		}

		if err := tx.Bucket(macroBucket).Delete(encID); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		return nil
	})
}
