package kv

import (
	"bytes"
	"context"
	"encoding/json"

	influxdb "github.com/influxdata/influxdb"
)

var (
	macroBucket    = []byte("macrosv1")
	macroOrgsIndex = []byte("macroorgsv1")
)

func (s *Service) initializeMacros(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(macroBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(macroOrgsIndex); err != nil {
		return err
	}
	return nil
}

func decodeMacroOrgsIndexKey(indexKey []byte) (orgID influxdb.ID, macroID influxdb.ID, err error) {
	if len(indexKey) != 2*influxdb.IDLength {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "malformed macro orgs index key (please report this error)",
		}
	}

	if err := (&orgID).Decode(indexKey[:influxdb.IDLength]); err != nil {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad org id",
			Err:  influxdb.ErrInvalidID,
		}
	}

	if err := (&macroID).Decode(indexKey[influxdb.IDLength:]); err != nil {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad macro id",
			Err:  influxdb.ErrInvalidID,
		}
	}

	return orgID, macroID, nil
}

func (s *Service) findOrganizationMacros(ctx context.Context, tx Tx, orgID influxdb.ID) ([]*influxdb.Macro, error) {
	idx, err := tx.Bucket(macroOrgsIndex)
	if err != nil {
		return nil, err
	}

	// TODO(leodido): support find options
	cur, err := idx.Cursor()
	if err != nil {
		return nil, err
	}

	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	macros := []*influxdb.Macro{}
	for k, _ := cur.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		_, id, err := decodeMacroOrgsIndexKey(k)
		if err != nil {
			return nil, err
		}

		m, err := s.findMacroByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}

		macros = append(macros, m)
	}

	return macros, nil
}

func (s *Service) findMacros(ctx context.Context, tx Tx, filter influxdb.MacroFilter) ([]*influxdb.Macro, error) {
	if filter.OrganizationID != nil {
		return s.findOrganizationMacros(ctx, tx, *filter.OrganizationID)
	}

	if filter.Organization != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		return s.findOrganizationMacros(ctx, tx, o.ID)
	}

	macros := []*influxdb.Macro{}
	filterFn := filterMacrosFn(filter)
	err := s.forEachMacro(ctx, tx, func(m *influxdb.Macro) bool {
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

func filterMacrosFn(filter influxdb.MacroFilter) func(m *influxdb.Macro) bool {
	if filter.ID != nil {
		return func(m *influxdb.Macro) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *influxdb.Macro) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *influxdb.Macro) bool { return true }
}

// forEachMacro will iterate through all macros while fn returns true.
func (s *Service) forEachMacro(ctx context.Context, tx Tx, fn func(*influxdb.Macro) bool) error {
	b, err := tx.Bucket(macroBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &influxdb.Macro{}
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
func (s *Service) FindMacros(ctx context.Context, filter influxdb.MacroFilter, opt ...influxdb.FindOptions) ([]*influxdb.Macro, error) {
	// todo(leodido) > handle find options
	res := []*influxdb.Macro{}
	err := s.kv.View(func(tx Tx) error {
		macros, err := s.findMacros(ctx, tx, filter)
		if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
			return err
		}
		res = macros
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return res, nil
}

// FindMacroByID finds a single macro in the store by its ID
func (s *Service) FindMacroByID(ctx context.Context, id influxdb.ID) (*influxdb.Macro, error) {
	var macro *influxdb.Macro
	err := s.kv.View(func(tx Tx) error {
		m, pe := s.findMacroByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
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

func (s *Service) findMacroByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Macro, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(macroBucket)
	if err != nil {
		return nil, err
	}

	d, err := b.Get(encID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrMacroNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	macro := &influxdb.Macro{}
	err = json.Unmarshal(d, &macro)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return macro, nil
}

// CreateMacro creates a new macro and assigns it an ID
func (s *Service) CreateMacro(ctx context.Context, macro *influxdb.Macro) error {
	return s.kv.Update(func(tx Tx) error {
		macro.ID = s.IDGenerator.ID()

		if err := s.putMacroOrgsIndex(ctx, tx, macro); err != nil {
			return err
		}

		if pe := s.putMacro(ctx, tx, macro); pe != nil {
			return &influxdb.Error{
				Err: pe,
			}

		}
		return nil
	})
}

// ReplaceMacro puts a macro in the store
func (s *Service) ReplaceMacro(ctx context.Context, macro *influxdb.Macro) error {
	return s.kv.Update(func(tx Tx) error {
		if err := s.putMacroOrgsIndex(ctx, tx, macro); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		return s.putMacro(ctx, tx, macro)
	})
}

func encodeMacroOrgsIndex(macro *influxdb.Macro) ([]byte, error) {
	oID, err := macro.OrganizationID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Msg: "bad organization id",
		}
	}

	mID, err := macro.ID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Msg: "bad macro id",
		}
	}

	key := make([]byte, 0, influxdb.IDLength*2)
	key = append(key, oID...)
	key = append(key, mID...)

	return key, nil
}

func (s *Service) putMacroOrgsIndex(ctx context.Context, tx Tx, macro *influxdb.Macro) error {
	key, err := encodeMacroOrgsIndex(macro)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(macroOrgsIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(key, nil); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

func (s *Service) removeMacroOrgsIndex(ctx context.Context, tx Tx, macro *influxdb.Macro) error {
	key, err := encodeMacroOrgsIndex(macro)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(macroOrgsIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(key); err != nil {
		return err
	}

	return nil
}

func (s *Service) putMacro(ctx context.Context, tx Tx, macro *influxdb.Macro) error {
	m, err := json.Marshal(macro)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encID, err := macro.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(macroBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encID, m); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

// UpdateMacro updates a single macro in the store with a changeset
func (s *Service) UpdateMacro(ctx context.Context, id influxdb.ID, update *influxdb.MacroUpdate) (*influxdb.Macro, error) {
	var macro *influxdb.Macro
	err := s.kv.Update(func(tx Tx) error {
		m, pe := s.findMacroByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}

		if err := update.Apply(m); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		macro = m
		if pe = s.putMacro(ctx, tx, macro); pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
		return nil
	})

	return macro, err
}

// DeleteMacro removes a single macro from the store by its ID
func (s *Service) DeleteMacro(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(func(tx Tx) error {
		m, pe := s.findMacroByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}

		encID, err := id.Encode()
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		if err := s.removeMacroOrgsIndex(ctx, tx, m); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		b, err := tx.Bucket(macroBucket)
		if err != nil {
			return err
		}

		if err := b.Delete(encID); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		return nil
	})
}
