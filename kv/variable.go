package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
)

var (
	variableOrgsIndex = []byte("variableorgsv1")
)

func newVariableUniqueByNameStore(kv Store) *uniqByNameStore {
	return &uniqByNameStore{
		kv:              kv,
		caseInsensitive: true,
		resource:        "variable",
		bktName:         []byte("variablesv1"),
		indexName:       []byte("variablesindexv1"),
		decodeBucketEntFn: func(key, val []byte) ([]byte, interface{}, error) {
			var v influxdb.Variable
			return key, &v, json.Unmarshal(val, &v)
		},
		decodeOrgNameFn: func(body []byte) (influxdb.ID, string, error) {
			var v influxdb.Variable
			err := json.Unmarshal(body, &v)
			return v.ID, v.Name, err
		},
	}
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

	// TODO(jsteenb2): investigate why we don't implement the find options for vars?
	var o influxdb.FindOptions

	var variables []*influxdb.Variable
	err := s.variableStore.Find(ctx, tx, o, filterVariablesFn(filter), func(k []byte, v interface{}) {
		variables = append(variables, v.(*influxdb.Variable))
	})
	if err != nil {
		return nil, err
	}
	return variables, nil
}

func filterVariablesFn(filter influxdb.VariableFilter) func([]byte, interface{}) bool {
	return func(key []byte, val interface{}) bool {
		variable, ok := val.(*influxdb.Variable)
		if !ok {
			return false
		}

		if filter.ID != nil {
			return variable.ID == *filter.ID
		}

		if filter.OrganizationID != nil {
			return variable.OrganizationID == *filter.OrganizationID
		}

		return true
	}
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
	return res, err
}

// FindVariableByID finds a single variable in the store by its ID
func (s *Service) FindVariableByID(ctx context.Context, id influxdb.ID) (*influxdb.Variable, error) {
	var variable *influxdb.Variable
	err := s.kv.View(ctx, func(tx Tx) error {
		m, err := s.findVariableByID(ctx, tx, id)
		if err != nil {
			return err
		}
		variable = m
		return nil
	})
	return variable, err
}

func (s *Service) findVariableByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Variable, error) {
	body, err := s.variableStore.FindByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	var v influxdb.Variable
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}
	return &v, nil
}

// CreateVariable creates a new variable and assigns it an ID
func (s *Service) CreateVariable(ctx context.Context, v *influxdb.Variable) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := v.Valid(); err != nil {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Err:  err,
			}
		}

		v.Name = strings.TrimSpace(v.Name) // TODO: move to service layer

		if _, err := s.variableStore.FindByName(ctx, tx, v.OrganizationID, v.Name); err == nil {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("variable with name %s already exists", v.Name),
			}
		}

		v.ID = s.IDGenerator.ID()

		if err := s.putVariableOrgsIndex(ctx, tx, v); err != nil {
			return err
		}
		now := s.Now()
		v.CreatedAt = now
		v.UpdatedAt = now
		return s.putVariable(ctx, tx, v)
	})
}

// ReplaceVariable puts a variable in the store
func (s *Service) ReplaceVariable(ctx context.Context, v *influxdb.Variable) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		if err := s.putVariableOrgsIndex(ctx, tx, v); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		if _, err := s.variableStore.FindByName(ctx, tx, v.OrganizationID, v.Name); err == nil {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("variable with name %s already exists", v.Name),
			}
		}

		return s.putVariable(ctx, tx, v)
	})
}

func (s *Service) putVariableOrgsIndex(ctx context.Context, tx Tx, variable *influxdb.Variable) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

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

func (s *Service) putVariable(ctx context.Context, tx Tx, v *influxdb.Variable) error {
	m, err := json.Marshal(v)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}

	ent := Entity{
		ID:    v.ID,
		Name:  v.Name,
		OrgID: v.OrganizationID,
		Body:  m,
	}
	return s.variableStore.Put(ctx, tx, ent)
}

// UpdateVariable updates a single variable in the store with a changeset
func (s *Service) UpdateVariable(ctx context.Context, id influxdb.ID, update *influxdb.VariableUpdate) (*influxdb.Variable, error) {
	var v *influxdb.Variable
	err := s.kv.Update(ctx, func(tx Tx) error {
		m, err := s.findVariableByID(ctx, tx, id)
		if err != nil {
			return err
		}
		m.UpdatedAt = s.Now()

		v = m

		if update.Name != "" {
			// TODO: should be moved to service layer
			update.Name = strings.ToLower(strings.TrimSpace(update.Name))

			vbytes, err := s.variableStore.FindByName(ctx, tx, v.OrganizationID, update.Name)
			if err == nil {
				var existingVar influxdb.Variable
				if err := json.Unmarshal(vbytes, &existingVar); err != nil {
					return &influxdb.Error{
						Code: influxdb.EInternal,
						Err:  err,
					}
				}
				if existingVar.ID != v.ID {
					return &influxdb.Error{
						Code: influxdb.EConflict,
						Msg:  fmt.Sprintf("variable with name %s already exists", update.Name),
					}
				}
			}

			if err := s.variableStore.deleteInIndex(ctx, tx, v.OrganizationID, v.Name); err != nil {
				return err
			}
			v.Name = update.Name
		}

		update.Apply(m)

		return s.putVariable(ctx, tx, v)
	})

	return v, err
}

// DeleteVariable removes a single variable from the store by its ID
func (s *Service) DeleteVariable(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		v, err := s.findVariableByID(ctx, tx, id)
		if err != nil {
			return err
		}

		if err := s.removeVariableOrgsIndex(ctx, tx, v); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}

		return s.variableStore.Delete(ctx, tx, id)
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

	key := make([]byte, influxdb.IDLength*2)
	copy(key, oID)
	copy(key[influxdb.IDLength:], mID)

	return key, nil
}
