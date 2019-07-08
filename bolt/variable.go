package bolt

import (
	"bytes"
	"context"
	"encoding/json"

	bolt "github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	variableBucket    = []byte("variablesv1")
	variableOrgsIndex = []byte("variableorgsv1")
)

func (c *Client) initializeVariables(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(variableBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(variableOrgsIndex); err != nil {
		return err
	}
	return nil
}

func decodeVariableOrgsIndexKey(indexKey []byte) (orgID platform.ID, variableID platform.ID, err error) {
	if len(indexKey) != 2*platform.IDLength {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "malformed variable orgs index key (please report this error)",
		}
	}

	if err := (&orgID).Decode(indexKey[:platform.IDLength]); err != nil {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "bad org id",
			Err:  platform.ErrInvalidID,
		}
	}

	if err := (&variableID).Decode(indexKey[platform.IDLength:]); err != nil {
		return 0, 0, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "bad variable id",
			Err:  platform.ErrInvalidID,
		}
	}

	return orgID, variableID, nil
}

func (c *Client) findOrganizationVariables(ctx context.Context, tx *bolt.Tx, orgID platform.ID) ([]*platform.Variable, error) {
	// TODO(leodido): support find options
	cur := tx.Bucket(variableOrgsIndex).Cursor()
	prefix, err := orgID.Encode()
	if err != nil {
		return nil, err
	}

	variables := []*platform.Variable{}
	for k, _ := cur.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = cur.Next() {
		_, id, err := decodeVariableOrgsIndexKey(k)
		if err != nil {
			return nil, err
		}

		m, err := c.findVariableByID(ctx, tx, id)
		if err != nil {
			return nil, err
		}

		variables = append(variables, m)
	}

	return variables, nil
}

func (c *Client) findVariables(ctx context.Context, tx *bolt.Tx, filter platform.VariableFilter) ([]*platform.Variable, error) {
	if filter.OrganizationID != nil {
		return c.findOrganizationVariables(ctx, tx, *filter.OrganizationID)
	}

	if filter.Organization != nil {
		o, err := c.findOrganizationByName(ctx, tx, *filter.Organization)
		if err != nil {
			return nil, err
		}
		return c.findOrganizationVariables(ctx, tx, o.ID)
	}

	variables := []*platform.Variable{}
	filterFn := filterVariablesFn(filter)
	err := c.forEachVariable(ctx, tx, func(m *platform.Variable) bool {
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

func filterVariablesFn(filter platform.VariableFilter) func(m *platform.Variable) bool {
	if filter.ID != nil {
		return func(m *platform.Variable) bool {
			return m.ID == *filter.ID
		}
	}

	if filter.OrganizationID != nil {
		return func(m *platform.Variable) bool {
			return m.OrganizationID == *filter.OrganizationID
		}
	}

	return func(m *platform.Variable) bool { return true }
}

// forEachVariable will iterate through all variables while fn returns true.
func (c *Client) forEachVariable(ctx context.Context, tx *bolt.Tx, fn func(*platform.Variable) bool) error {
	cur := tx.Bucket(variableBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		m := &platform.Variable{}
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
func (c *Client) FindVariables(ctx context.Context, filter platform.VariableFilter, opt ...platform.FindOptions) ([]*platform.Variable, error) {
	// todo(leodido) > handle find options
	op := getOp(platform.OpFindVariables)
	res := []*platform.Variable{}
	err := c.db.View(func(tx *bolt.Tx) error {
		variables, err := c.findVariables(ctx, tx, filter)
		if err != nil && platform.ErrorCode(err) != platform.ENotFound {
			return err
		}
		res = variables
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

// FindVariableByID finds a single variable in the store by its ID
func (c *Client) FindVariableByID(ctx context.Context, id platform.ID) (*platform.Variable, error) {
	op := getOp(platform.OpFindVariableByID)
	var variable *platform.Variable
	err := c.db.View(func(tx *bolt.Tx) error {
		m, pe := c.findVariableByID(ctx, tx, id)
		if pe != nil {
			return &platform.Error{
				Op:  op,
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

func (c *Client) findVariableByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Variable, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	d := tx.Bucket(variableBucket).Get(encID)
	if d == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  platform.ErrVariableNotFound,
		}
	}

	variable := &platform.Variable{}
	err = json.Unmarshal(d, &variable)
	if err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return variable, nil
}

// CreateVariable creates a new variable and assigns it an ID
func (c *Client) CreateVariable(ctx context.Context, variable *platform.Variable) error {
	op := getOp(platform.OpCreateVariable)
	return c.db.Update(func(tx *bolt.Tx) error {
		variable.ID = c.IDGenerator.ID()

		if err := c.putVariableOrgsIndex(ctx, tx, variable); err != nil {
			return err
		}

		if pe := c.putVariable(ctx, tx, variable); pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}

		}
		return nil
	})
}

// ReplaceVariable puts a variable in the store
func (c *Client) ReplaceVariable(ctx context.Context, variable *platform.Variable) error {
	op := getOp(platform.OpReplaceVariable)
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.putVariableOrgsIndex(ctx, tx, variable); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}
		return c.putVariable(ctx, tx, variable)
	})
}

func encodeVariableOrgsIndex(variable *platform.Variable) ([]byte, error) {
	oID, err := variable.OrganizationID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Msg: "bad organization id",
		}
	}

	mID, err := variable.ID.Encode()
	if err != nil {
		return nil, &platform.Error{
			Err: err,
			Msg: "bad variable id",
		}
	}

	key := make([]byte, 0, platform.IDLength*2)
	key = append(key, oID...)
	key = append(key, mID...)

	return key, nil
}

func (c *Client) putVariableOrgsIndex(ctx context.Context, tx *bolt.Tx, variable *platform.Variable) error {
	key, err := encodeVariableOrgsIndex(variable)
	if err != nil {
		return err
	}

	if err := tx.Bucket(variableOrgsIndex).Put(key, nil); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

func (c *Client) removeVariableOrgsIndex(ctx context.Context, tx *bolt.Tx, variable *platform.Variable) error {
	key, err := encodeVariableOrgsIndex(variable)
	if err != nil {
		return err
	}

	if err := tx.Bucket(variableOrgsIndex).Delete(key); err != nil {
		return err
	}

	return nil
}

func (c *Client) putVariable(ctx context.Context, tx *bolt.Tx, variable *platform.Variable) error {
	m, err := json.Marshal(variable)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	encID, err := variable.ID.Encode()
	if err != nil {
		return &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	if err := tx.Bucket(variableBucket).Put(encID, m); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

// UpdateVariable updates a single variable in the store with a changeset
func (c *Client) UpdateVariable(ctx context.Context, id platform.ID, update *platform.VariableUpdate) (*platform.Variable, error) {
	op := getOp(platform.OpUpdateVariable)
	var variable *platform.Variable
	err := c.db.Update(func(tx *bolt.Tx) error {
		m, pe := c.findVariableByID(ctx, tx, id)
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

		variable = m
		if pe = c.putVariable(ctx, tx, variable); pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}
		return nil
	})

	return variable, err
}

// DeleteVariable removes a single variable from the store by its ID
func (c *Client) DeleteVariable(ctx context.Context, id platform.ID) error {
	op := getOp(platform.OpDeleteVariable)
	return c.db.Update(func(tx *bolt.Tx) error {
		m, pe := c.findVariableByID(ctx, tx, id)
		if pe != nil {
			return &platform.Error{
				Op:  op,
				Err: pe,
			}
		}

		encID, err := id.Encode()
		if err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		if err := c.removeVariableOrgsIndex(ctx, tx, m); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		if err := tx.Bucket(variableBucket).Delete(encID); err != nil {
			return &platform.Error{
				Op:  op,
				Err: err,
			}
		}

		return nil
	})
}
