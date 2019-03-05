package kv

import (
	"bytes"
	"context"
	"encoding/json"

	influxdb "github.com/influxdata/influxdb"
)

var (
	variableBucket    = []byte("variablesv1")
	variableOrgsIndex = []byte("variableorgsv1")
)

func (s *Service) initializeVariables(ctx context.Context, tx Tx) error {
	if _, err := tx.Bucket(variableBucket); err != nil {
		return err
	}
	if _, err := tx.Bucket(variableOrgsIndex); err != nil {
		return err
	}
	return nil
}

func decodeVariableOrgsIndexKey(indexKey []byte) (orgID influxdb.ID, variableID influxdb.ID, err error) {
	if len(indexKey) != 2*influxdb.IDLength {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "malformed variable orgs index key (please report this error)",
		}
	}

	if err := (&orgID).Decode(indexKey[:influxdb.IDLength]); err != nil {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad org id",
			Err:  influxdb.ErrInvalidID,
		}
	}

	if err := (&variableID).Decode(indexKey[influxdb.IDLength:]); err != nil {
		return 0, 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad variable id",
			Err:  influxdb.ErrInvalidID,
		}
	}

	return orgID, variableID, nil
}

func (s *Service) findOrganizationVariables(ctx context.Context, tx Tx, orgID influxdb.ID) ([]*influxdb.Variable, error) {
	idx, err := tx.Bucket(variableOrgsIndex)
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

	variables := []*influxdb.Variable{}
	for k, _ := cur.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		_, id, err := decodeVariableOrgsIndexKey(k)
		if err != nil {
			return nil, err
		}

		m, err := s.findVariableByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}

		variables = append(variables, m)
	}

	return variables, nil
}

func (s *Service) findVariables(ctx context.Context, tx Tx, filter influxdb.VariableFilter) ([]*influxdb.Variable, error) {
	if filter.OrganizationID != nil {
		return s.findOrganizationVariables(ctx, tx, *filter.OrganizationID)
	}

	if filter.Organization != nil {
		o, err := s.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		return s.findOrganizationVariables(ctx, tx, o.ID)
	}

	variables := []*influxdb.Variable{}
	filterFn := filterVariablesFn(filter)
	err := s.forEachVariable(ctx, tx, func(m *influxdb.Variable) bool {
		if filterFn(m) {
			variables = append(variables, m)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return variables, nil
}

func filterVariablesFn(filter influxdb.VariableFilter) func(m *influxdb.Variable) bool {
	if filter.ID != nil {
		return func(m *influxdb.Variable) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *influxdb.Variable) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *influxdb.Variable) bool { return true }
}

// forEachVariable will iterate through all variables while fn returns true.
func (s *Service) forEachVariable(ctx context.Context, tx Tx, fn func(*influxdb.Variable) bool) error {
	b, err := tx.Bucket(variableBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &influxdb.Variable{}
		if err := json.Unmarshal(v, m); err != nil {
			return err
		}
		if !fn(m) {
			break
		}
	}

	return nil
}

// FindVariables returns all variables in the store
func (s *Service) FindVariables(ctx context.Context, filter influxdb.VariableFilter, opt ...influxdb.FindOptions) ([]*influxdb.Variable, error) {
	// todo(leodido) > handle find options
	res := []*influxdb.Variable{}
	err := s.kv.View(ctx, func(tx Tx) error {
		variables, err := s.findVariables(ctx, tx, filter)
		if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
			return err
		}
		res = variables
		return nil
	})

	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return res, nil
}

// FindVariableByID finds a single variable in the store by its ID
func (s *Service) FindVariableByID(ctx context.Context, id influxdb.ID) (*influxdb.Variable, error) {
	var variable *influxdb.Variable
	err := s.kv.View(ctx, func(tx Tx) error {
		m, pe := s.findVariableByID(ctx, tx, id)
		if pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
		variable = m
		return nil
	})
	if err != nil {
		return nil, err
	}

	return variable, nil
}

func (s *Service) findVariableByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Variable, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(variableBucket)
	if err != nil {
		return nil, err
	}

	d, err := b.Get(encID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  influxdb.ErrVariableNotFound,
		}
	}

	if err != nil {
		return nil, err
	}

	variable := &influxdb.Variable{}
	err = json.Unmarshal(d, &variable)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return variable, nil
}

// CreateVariable creates a new variable and assigns it an ID
func (s *Service) CreateVariable(ctx context.Context, variable *influxdb.Variable) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		variable.ID = s.IDGenerator.ID()

		if err := s.putVariableOrgsIndex(ctx, tx, variable); err != nil {
			return err
		}

		if pe := s.putVariable(ctx, tx, variable); pe != nil {
			return &influxdb.Error{
				Err: pe,
			}

		}
		return nil
	})
}

// ReplaceVariable puts a variable in the store
func (s *Service) ReplaceVariable(ctx context.Context, variable *influxdb.Variable) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := s.putVariableOrgsIndex(ctx, tx, variable); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		return s.putVariable(ctx, tx, variable)
	})
}

func encodeVariableOrgsIndex(variable *influxdb.Variable) ([]byte, error) {
	oID, err := variable.OrganizationID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Msg: "bad organization id",
		}
	}

	mID, err := variable.ID.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Msg: "bad variable id",
		}
	}

	key := make([]byte, 0, influxdb.IDLength*2)
	key = append(key, oID...)
	key = append(key, mID...)

	return key, nil
}

func (s *Service) putVariableOrgsIndex(ctx context.Context, tx Tx, variable *influxdb.Variable) error {
	key, err := encodeVariableOrgsIndex(variable)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(variableOrgsIndex)
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

func (s *Service) removeVariableOrgsIndex(ctx context.Context, tx Tx, variable *influxdb.Variable) error {
	key, err := encodeVariableOrgsIndex(variable)
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(variableOrgsIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(key); err != nil {
		return err
	}

	return nil
}

func (s *Service) putVariable(ctx context.Context, tx Tx, variable *influxdb.Variable) error {
	m, err := json.Marshal(variable)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	encID, err := variable.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(variableBucket)
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

// UpdateVariable updates a single variable in the store with a changeset
func (s *Service) UpdateVariable(ctx context.Context, id influxdb.ID, update *influxdb.VariableUpdate) (*influxdb.Variable, error) {
	var variable *influxdb.Variable
	err := s.kv.Update(ctx, func(tx Tx) error {
		m, pe := s.findVariableByID(ctx, tx, id)
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

		variable = m
		if pe = s.putVariable(ctx, tx, variable); pe != nil {
			return &influxdb.Error{
				Err: pe,
			}
		}
		return nil
	})

	return variable, err
}

// DeleteVariable removes a single variable from the store by its ID
func (s *Service) DeleteVariable(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		m, pe := s.findVariableByID(ctx, tx, id)
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

		if err := s.removeVariableOrgsIndex(ctx, tx, m); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		b, err := tx.Bucket(variableBucket)
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
