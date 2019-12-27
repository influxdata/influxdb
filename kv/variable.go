package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
)

func newVariableUniqueByNameStore() *IndexStore {
	const resource = "variable"

	var decodeVarEntFn DecodeBucketEntFn = func(key, val []byte) ([]byte, interface{}, error) {
		var v influxdb.Variable
		return key, &v, json.Unmarshal(val, &v)
	}

	var decValToEntFn DecodedValToEntFn = func(_ []byte, i interface{}) (entity Entity, err error) {
		v, ok := i.(*influxdb.Variable)
		if err := errUnexpectedDecodeVal(ok); err != nil {
			return Entity{}, err
		}
		return Entity{
			ID:    v.ID,
			Name:  v.Name,
			OrgID: v.OrganizationID,
			Body:  v,
		}, nil
	}

	return &IndexStore{
		Resource:   resource,
		EntStore:   NewStoreBase(resource, []byte("variablesv1"), EncIDKey, EncBodyJSON, decodeVarEntFn, decValToEntFn),
		IndexStore: NewOrgNameKeyStore(resource, []byte("variablesindexv1"), false),
	}
}

func (s *Service) findOrganizationVariables(ctx context.Context, tx Tx, orgID influxdb.ID) ([]*influxdb.Variable, error) {
	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	variables := []*influxdb.Variable{}
	err = s.variableStore.Find(ctx, tx, FindOpts{
		Prefix: prefix,
		CaptureFn: func(key []byte, decodedVal interface{}) error {
			v, ok := decodedVal.(*influxdb.Variable)
			if err := errUnexpectedDecodeVal(ok); err != nil {
				return err
			}
			variables = append(variables, v)
			return nil
		},
	})
	if err != nil {
		return nil, err
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
	var variables []*influxdb.Variable
	err := s.variableStore.Find(ctx, tx, FindOpts{
		FilterFn: filterVariablesFn(filter),
		CaptureFn: func(key []byte, decodedVal interface{}) error {
			variables = append(variables, decodedVal.(*influxdb.Variable))
			return nil
		},
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
	body, err := s.variableStore.FindEnt(ctx, tx, Entity{ID: id})
	if err != nil {
		return nil, err
	}

	variable, ok := body.(*influxdb.Variable)
	return variable, errUnexpectedDecodeVal(ok)
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

		_, err := s.variableStore.FindEnt(ctx, tx, Entity{
			OrgID: v.OrganizationID,
			Name:  v.Name,
		})
		if err == nil {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("variable with name %s already exists", v.Name),
			}
		}

		v.ID = s.IDGenerator.ID()

		now := s.Now()
		v.CreatedAt = now
		v.UpdatedAt = now
		return s.putVariable(ctx, tx, v)
	})
}

// ReplaceVariable puts a variable in the store
func (s *Service) ReplaceVariable(ctx context.Context, v *influxdb.Variable) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		_, err := s.variableStore.FindEnt(ctx, tx, Entity{
			ID:    v.ID,
			OrgID: v.OrganizationID,
			Name:  v.Name,
		})
		if err == nil {
			return &influxdb.Error{
				Code: influxdb.EConflict,
				Msg:  fmt.Sprintf("variable with name %s already exists", v.Name),
			}
		}

		return s.putVariable(ctx, tx, v)
	})
}

func (s *Service) putVariable(ctx context.Context, tx Tx, v *influxdb.Variable) error {
	ent := Entity{
		ID:    v.ID,
		Name:  v.Name,
		OrgID: v.OrganizationID,
		Body:  v,
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

			vbytes, err := s.variableStore.FindEnt(ctx, tx, Entity{
				OrgID: v.OrganizationID,
				Name:  update.Name,
			})
			if err == nil {
				existingVar, ok := vbytes.(*influxdb.Variable)
				if err := errUnexpectedDecodeVal(ok); err != nil {
					return err
				}
				if existingVar.ID != v.ID {
					return &influxdb.Error{
						Code: influxdb.EConflict,
						Msg:  fmt.Sprintf("variable with name %s already exists", update.Name),
					}
				}
			}

			err = s.variableStore.IndexStore.DeleteEnt(ctx, tx, Entity{
				OrgID: v.OrganizationID,
				Name:  v.Name,
			})
			if err != nil {
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
		return s.variableStore.DeleteEnt(ctx, tx, Entity{ID: id})
	})
}
