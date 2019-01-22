package bolt

import (
	"context"
	"encoding/json"

	"github.com/coreos/bbolt"
	platform "github.com/influxdata/influxdb"
)

var (
	authorizationBucket = []byte("authorizationsv1")
	authorizationIndex  = []byte("authorizationindexv1")
)

var _ platform.AuthorizationService = (*Client)(nil)

func (c *Client) initializeAuthorizations(ctx context.Context, tx *bolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists([]byte(authorizationBucket)); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists([]byte(authorizationIndex)); err != nil {
		return err
	}
	return nil
}

// FindAuthorizationByID retrieves a authorization by id.
func (c *Client) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	var a *platform.Authorization
	var err error
	err = c.db.View(func(tx *bolt.Tx) error {
		var pe *platform.Error
		a, pe = c.findAuthorizationByID(ctx, tx, id)
		if pe != nil {
			pe.Op = getOp(platform.OpFindAuthorizationByID)
			err = pe
		}
		return err
	})

	return a, err
}

func (c *Client) findAuthorizationByID(ctx context.Context, tx *bolt.Tx, id platform.ID) (*platform.Authorization, *platform.Error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	var a platform.Authorization
	v := tx.Bucket(authorizationBucket).Get(encodedID)

	if len(v) == 0 {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "authorization not found",
		}
	}

	if err := decodeAuthorization(v, &a); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	return &a, nil
}

// FindAuthorizationByToken returns a authorization by token for a particular authorization.
func (c *Client) FindAuthorizationByToken(ctx context.Context, n string) (*platform.Authorization, error) {
	var a *platform.Authorization
	var err error
	err = c.db.View(func(tx *bolt.Tx) error {
		var pe *platform.Error
		a, pe = c.findAuthorizationByToken(ctx, tx, n)
		if pe != nil {
			pe.Op = getOp(platform.OpFindAuthorizationByToken)
			err = pe
		}
		return err
	})

	return a, err
}

func (c *Client) findAuthorizationByToken(ctx context.Context, tx *bolt.Tx, n string) (*platform.Authorization, *platform.Error) {
	a := tx.Bucket(authorizationIndex).Get(authorizationIndexKey(n))
	if a == nil {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "authorization not found",
		}
	}
	var id platform.ID
	if err := id.Decode(a); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}
	return c.findAuthorizationByID(ctx, tx, id)
}

func filterAuthorizationsFn(filter platform.AuthorizationFilter) func(a *platform.Authorization) bool {
	if filter.ID != nil {
		return func(a *platform.Authorization) bool {
			return a.ID == *filter.ID
		}
	}

	if filter.Token != nil {
		return func(a *platform.Authorization) bool {
			return a.Token == *filter.Token
		}
	}

	if filter.UserID != nil {
		return func(a *platform.Authorization) bool {
			return a.UserID == *filter.UserID
		}
	}

	return func(a *platform.Authorization) bool { return true }
}

// FindAuthorizations retrives all authorizations that match an arbitrary authorization filter.
// Filters using ID, or Token should be efficient.
// Other filters will do a linear scan across all authorizations searching for a match.
func (c *Client) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	if filter.ID != nil {
		a, err := c.FindAuthorizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  getOp(platform.OpFindAuthorizations),
			}
		}

		return []*platform.Authorization{a}, 1, nil
	}

	if filter.Token != nil {
		a, err := c.FindAuthorizationByToken(ctx, *filter.Token)
		if err != nil {
			return nil, 0, &platform.Error{
				Err: err,
				Op:  getOp(platform.OpFindAuthorizations),
			}
		}

		return []*platform.Authorization{a}, 1, nil
	}

	as := []*platform.Authorization{}
	err := c.db.View(func(tx *bolt.Tx) error {
		auths, err := c.findAuthorizations(ctx, tx, filter)
		if err != nil {
			return err
		}
		as = auths
		return nil
	})

	if err != nil {
		return nil, 0, &platform.Error{
			Err: err,
			Op:  getOp(platform.OpFindAuthorizations),
		}
	}

	return as, len(as), nil
}

func (c *Client) findAuthorizations(ctx context.Context, tx *bolt.Tx, f platform.AuthorizationFilter) ([]*platform.Authorization, error) {
	// If the users name was provided, look up user by ID first
	if f.User != nil {
		u, err := c.findUserByName(ctx, tx, *f.User)
		if err != nil {
			return nil, err
		}
		f.UserID = &u.ID
	}

	as := []*platform.Authorization{}
	filterFn := filterAuthorizationsFn(f)
	err := c.forEachAuthorization(ctx, tx, func(a *platform.Authorization) bool {
		if filterFn(a) {
			as = append(as, a)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return as, nil
}

// CreateAuthorization creates a platform authorization and sets b.ID, and b.UserID if not provided.
func (c *Client) CreateAuthorization(ctx context.Context, a *platform.Authorization) error {
	op := getOp(platform.OpCreateAuthorization)
	if err := a.Valid(); err != nil {
		return &platform.Error{
			Err: err,
			Op:  op,
		}
	}

	return c.db.Update(func(tx *bolt.Tx) error {
		_, pErr := c.findUserByID(ctx, tx, a.UserID)
		if pErr != nil {
			return platform.ErrUnableToCreateToken
		}

		_, pErr = c.findOrganizationByID(ctx, tx, a.OrgID)
		if pErr != nil {
			return platform.ErrUnableToCreateToken
		}

		if unique := c.uniqueAuthorizationToken(ctx, tx, a); !unique {
			return platform.ErrUnableToCreateToken
		}

		if a.Token == "" {
			token, err := c.TokenGenerator.Token()
			if err != nil {
				return &platform.Error{
					Err: err,
					Op:  op,
				}
			}
			a.Token = token
		}

		a.ID = c.IDGenerator.ID()

		pe := c.putAuthorization(ctx, tx, a)
		if pe != nil {
			pe.Op = op
			return pe
		}

		return nil
	})
}

// PutAuthorization will put a authorization without setting an ID.
func (c *Client) PutAuthorization(ctx context.Context, a *platform.Authorization) (err error) {
	return c.db.Update(func(tx *bolt.Tx) error {
		pe := c.putAuthorization(ctx, tx, a)
		if pe != nil {
			err = pe
		}
		return err
	})
}

func encodeAuthorization(a *platform.Authorization) ([]byte, error) {
	switch a.Status {
	case platform.Active, platform.Inactive:
	case "":
		a.Status = platform.Active
	default:
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "unknown authorization status",
		}
	}

	return json.Marshal(a)
}

func (c *Client) putAuthorization(ctx context.Context, tx *bolt.Tx, a *platform.Authorization) *platform.Error {
	v, err := encodeAuthorization(a)
	if err != nil {
		return &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	encodedID, err := a.ID.Encode()
	if err != nil {
		return &platform.Error{
			Code: platform.ENotFound,
			Err:  err,
		}
	}

	if err := tx.Bucket(authorizationIndex).Put(authorizationIndexKey(a.Token), encodedID); err != nil {
		return &platform.Error{
			Code: platform.EInternal,
			Err:  err,
		}
	}

	if err := tx.Bucket(authorizationBucket).Put(encodedID, v); err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	return nil
}

func authorizationIndexKey(n string) []byte {
	return []byte(n)
}

func decodeAuthorization(b []byte, a *platform.Authorization) error {
	if err := json.Unmarshal(b, a); err != nil {
		return err
	}
	if a.Status == "" {
		a.Status = platform.Active
	}
	return nil
}

// forEachAuthorization will iterate through all authorizations while fn returns true.
func (c *Client) forEachAuthorization(ctx context.Context, tx *bolt.Tx, fn func(*platform.Authorization) bool) error {
	cur := tx.Bucket(authorizationBucket).Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		a := &platform.Authorization{}

		if err := decodeAuthorization(v, a); err != nil {
			return err
		}
		if !fn(a) {
			break
		}
	}

	return nil
}

func (c *Client) uniqueAuthorizationToken(ctx context.Context, tx *bolt.Tx, a *platform.Authorization) bool {
	v := tx.Bucket(authorizationIndex).Get(authorizationIndexKey(a.Token))
	return len(v) == 0
}

// DeleteAuthorization deletes a authorization and prunes it from the index.
func (c *Client) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	err := c.db.Update(func(tx *bolt.Tx) (err error) {
		pe := c.deleteAuthorization(ctx, tx, id)
		if pe != nil {
			pe.Op = getOp(platform.OpDeleteAuthorization)
			err = pe
		}
		return err
	})
	return err
}

func (c *Client) deleteAuthorization(ctx context.Context, tx *bolt.Tx, id platform.ID) *platform.Error {
	a, pe := c.findAuthorizationByID(ctx, tx, id)
	if pe != nil {
		return pe
	}
	if err := tx.Bucket(authorizationIndex).Delete(authorizationIndexKey(a.Token)); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	if err := tx.Bucket(authorizationBucket).Delete(encodedID); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return nil
}

// SetAuthorizationStatus updates the status of the authorization. Useful
// for setting an authorization to inactive or active.
func (c *Client) SetAuthorizationStatus(ctx context.Context, id platform.ID, status platform.Status) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if pe := c.updateAuthorization(ctx, tx, id, status); pe != nil {
			return &platform.Error{
				Err: pe,
				Op:  platform.OpSetAuthorizationStatus,
			}
		}
		return nil
	})
}

func (c *Client) updateAuthorization(ctx context.Context, tx *bolt.Tx, id platform.ID, status platform.Status) *platform.Error {
	a, pe := c.findAuthorizationByID(ctx, tx, id)
	if pe != nil {
		return pe
	}

	a.Status = status
	b, err := encodeAuthorization(a)
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	encodedID, err := id.Encode()
	if err != nil {
		return &platform.Error{
			Err: err,
		}
	}

	if err = tx.Bucket(authorizationBucket).Put(encodedID, b); err != nil {
		return &platform.Error{
			Err: err,
		}
	}
	return nil
}
