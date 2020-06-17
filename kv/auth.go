package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"
	influxdb "github.com/influxdata/influxdb/v2"
	jsonp "github.com/influxdata/influxdb/v2/pkg/jsonparser"
)

var (
	authBucket = []byte("authorizationsv1")
	authIndex  = []byte("authorizationindexv1")
)

var _ influxdb.AuthorizationService = (*Service)(nil)

// FindAuthorizationByID retrieves a authorization by id.
func (s *Service) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
	var a *influxdb.Authorization
	err := s.kv.View(ctx, func(tx Tx) error {
		auth, err := s.findAuthorizationByID(ctx, tx, id)
		if err != nil {
			return err
		}

		a = auth
		return nil
	})

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (s *Service) findAuthorizationByID(ctx context.Context, tx Tx, id influxdb.ID) (*influxdb.Authorization, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "authorization not found",
		}
	}

	if err != nil {
		return nil, err
	}

	a := &influxdb.Authorization{}
	if err := decodeAuthorization(v, a); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return a, nil
}

// FindAuthorizationByToken returns a authorization by token for a particular authorization.
func (s *Service) FindAuthorizationByToken(ctx context.Context, n string) (*influxdb.Authorization, error) {
	var a *influxdb.Authorization
	err := s.kv.View(ctx, func(tx Tx) error {
		auth, err := s.findAuthorizationByToken(ctx, tx, n)
		if err != nil {
			return err
		}

		a = auth

		return nil
	})

	if err != nil {
		return nil, err
	}

	return a, nil
}

func (s *Service) findAuthorizationByToken(ctx context.Context, tx Tx, n string) (*influxdb.Authorization, error) {
	idx, err := authIndexBucket(tx)
	if err != nil {
		return nil, err
	}

	a, err := idx.Get(authIndexKey(n))
	if IsNotFound(err) {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "authorization not found",
		}
	}

	var id influxdb.ID
	if err := id.Decode(a); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return s.findAuthorizationByID(ctx, tx, id)
}

func authorizationsPredicateFn(f influxdb.AuthorizationFilter) CursorPredicateFunc {
	// if any errors occur reading the JSON data, the predicate will always return true
	// to ensure the value is included and handled higher up.

	if f.ID != nil {
		exp := *f.ID
		return func(_, value []byte) bool {
			got, err := jsonp.GetID(value, "id")
			if err != nil {
				return true
			}
			return got == exp
		}
	}

	if f.Token != nil {
		exp := *f.Token
		return func(_, value []byte) bool {
			// it is assumed that token never has escaped string data
			got, _, _, err := jsonparser.Get(value, "token")
			if err != nil {
				return true
			}
			return string(got) == exp
		}
	}

	var pred CursorPredicateFunc
	if f.OrgID != nil {
		exp := *f.OrgID
		pred = func(_, value []byte) bool {
			got, err := jsonp.GetID(value, "orgID")
			if err != nil {
				return true
			}

			return got == exp
		}
	}

	if f.UserID != nil {
		exp := *f.UserID
		prevFn := pred
		pred = func(key, value []byte) bool {
			prev := prevFn == nil || prevFn(key, value)
			got, exists, err := jsonp.GetOptionalID(value, "userID")
			return prev && ((exp == got && exists) || err != nil)
		}
	}

	return pred
}

func filterAuthorizationsFn(filter influxdb.AuthorizationFilter) func(a *influxdb.Authorization) bool {
	if filter.ID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.ID == *filter.ID
		}
	}

	if filter.Token != nil {
		return func(a *influxdb.Authorization) bool {
			return a.Token == *filter.Token
		}
	}

	// Filter by org and user
	if filter.OrgID != nil && filter.UserID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.OrgID == *filter.OrgID && a.UserID == *filter.UserID
		}
	}

	if filter.OrgID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.OrgID == *filter.OrgID
		}
	}

	if filter.UserID != nil {
		return func(a *influxdb.Authorization) bool {
			return a.UserID == *filter.UserID
		}
	}

	return func(a *influxdb.Authorization) bool { return true }
}

// FindAuthorizations retrives all authorizations that match an arbitrary authorization filter.
// Filters using ID, or Token should be efficient.
// Other filters will do a linear scan across all authorizations searching for a match.
func (s *Service) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	if filter.ID != nil {
		a, err := s.FindAuthorizationByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}

		return []*influxdb.Authorization{a}, 1, nil
	}

	if filter.Token != nil {
		a, err := s.FindAuthorizationByToken(ctx, *filter.Token)
		if err != nil {
			return nil, 0, &influxdb.Error{
				Err: err,
			}
		}

		return []*influxdb.Authorization{a}, 1, nil
	}

	as := []*influxdb.Authorization{}
	err := s.kv.View(ctx, func(tx Tx) error {
		auths, err := s.findAuthorizations(ctx, tx, filter)
		if err != nil {
			return err
		}
		as = auths
		return nil
	})

	if err != nil {
		return nil, 0, &influxdb.Error{
			Err: err,
		}
	}

	return as, len(as), nil
}

func (s *Service) findAuthorizations(ctx context.Context, tx Tx, f influxdb.AuthorizationFilter) ([]*influxdb.Authorization, error) {
	// If the users name was provided, look up user by ID first
	if f.User != nil {
		u, err := s.findUserByName(ctx, tx, *f.User)
		if err != nil {
			return nil, err
		}
		f.UserID = &u.ID
	}

	if f.Org != nil {
		o, err := s.findOrganizationByName(ctx, tx, *f.Org)
		if err != nil {
			return nil, err
		}
		f.OrgID = &o.ID
	}

	var as []*influxdb.Authorization
	pred := authorizationsPredicateFn(f)
	filterFn := filterAuthorizationsFn(f)
	err := s.forEachAuthorization(ctx, tx, pred, func(a *influxdb.Authorization) bool {
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

// CreateAuthorization creates a influxdb authorization and sets b.ID, and b.UserID if not provided.
func (s *Service) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.createAuthorization(ctx, tx, a)
	})
}

// CreateAuthorizationTx is used when importing kv as a library
func (s *Service) CreateAuthorizationTx(ctx context.Context, tx Tx, a *influxdb.Authorization) error {
	return s.createAuthorization(ctx, tx, a)
}

func (s *Service) createAuthorization(ctx context.Context, tx Tx, a *influxdb.Authorization) error {
	if err := a.Valid(); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	if _, err := s.findUserByID(ctx, tx, a.UserID); err != nil {
		return influxdb.ErrUnableToCreateToken
	}

	if _, err := s.findOrganizationByID(ctx, tx, a.OrgID); err != nil {
		return influxdb.ErrUnableToCreateToken
	}

	if err := s.uniqueAuthToken(ctx, tx, a); err != nil {
		return err
	}

	if a.Token == "" {
		token, err := s.TokenGenerator.Token()
		if err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
		a.Token = token
	}

	a.ID = s.IDGenerator.ID()

	now := s.TimeGenerator.Now()
	a.SetCreatedAt(now)
	a.SetUpdatedAt(now)

	if err := s.putAuthorization(ctx, tx, a); err != nil {
		return err
	}

	return nil
}

// PutAuthorization will put a authorization without setting an ID.
func (s *Service) PutAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		return s.putAuthorization(ctx, tx, a)
	})
}

func encodeAuthorization(a *influxdb.Authorization) ([]byte, error) {
	switch a.Status {
	case influxdb.Active, influxdb.Inactive:
	case "":
		a.Status = influxdb.Active
	default:
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "unknown authorization status",
		}
	}

	return json.Marshal(a)
}

func (s *Service) putAuthorization(ctx context.Context, tx Tx, a *influxdb.Authorization) error {
	v, err := encodeAuthorization(a)
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	encodedID, err := a.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.ENotFound,
			Err:  err,
		}
	}

	idx, err := authIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Put(authIndexKey(a.Token), encodedID); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInternal,
			Err:  err,
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := b.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	return nil
}

func authIndexKey(n string) []byte {
	return []byte(n)
}

func decodeAuthorization(b []byte, a *influxdb.Authorization) error {
	if err := json.Unmarshal(b, a); err != nil {
		return err
	}
	if a.Status == "" {
		a.Status = influxdb.Active
	}
	return nil
}

// forEachAuthorization will iterate through all authorizations while fn returns true.
func (s *Service) forEachAuthorization(ctx context.Context, tx Tx, pred CursorPredicateFunc, fn func(*influxdb.Authorization) bool) error {
	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	var cur Cursor
	if pred != nil {
		cur, err = b.Cursor(WithCursorHintPredicate(pred))
	} else {
		cur, err = b.Cursor()
	}
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		// preallocate Permissions to reduce multiple slice re-allocations
		a := &influxdb.Authorization{
			Permissions: make([]influxdb.Permission, 64),
		}

		if err := decodeAuthorization(v, a); err != nil {
			return err
		}
		if !fn(a) {
			break
		}
	}

	return nil
}

// DeleteAuthorization deletes a authorization and prunes it from the index.
func (s *Service) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	return s.kv.Update(ctx, func(tx Tx) (err error) {
		return s.deleteAuthorization(ctx, tx, id)
	})
}

func (s *Service) deleteAuthorization(ctx context.Context, tx Tx, id influxdb.ID) error {
	a, err := s.findAuthorizationByID(ctx, tx, id)
	if err != nil {
		return err
	}

	idx, err := authIndexBucket(tx)
	if err != nil {
		return err
	}

	if err := idx.Delete(authIndexKey(a.Token)); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	encodedID, err := id.Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}

	b, err := tx.Bucket(authBucket)
	if err != nil {
		return err
	}

	if err := b.Delete(encodedID); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}

// UpdateAuthorization updates the status and description if available.
func (s *Service) UpdateAuthorization(ctx context.Context, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	var a *influxdb.Authorization
	var err error
	err = s.kv.Update(ctx, func(tx Tx) error {
		a, err = s.updateAuthorization(ctx, tx, id, upd)
		return err
	})
	return a, err
}

func (s *Service) updateAuthorization(ctx context.Context, tx Tx, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	a, err := s.findAuthorizationByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Status != nil {
		a.Status = *upd.Status
	}
	if upd.Description != nil {
		a.Description = *upd.Description
	}

	now := s.TimeGenerator.Now()
	a.SetUpdatedAt(now)

	if err := s.putAuthorization(ctx, tx, a); err != nil {
		return nil, err
	}

	return a, nil
}

func authIndexBucket(tx Tx) (Bucket, error) {
	b, err := tx.Bucket([]byte(authIndex))
	if err != nil {
		return nil, UnexpectedAuthIndexError(err)
	}

	return b, nil
}

// UnexpectedAuthIndexError is used when the error comes from an internal system.
func UnexpectedAuthIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving auth index; Err: %v", err),
		Op:   "kv/authIndex",
	}
}

func (s *Service) uniqueAuthToken(ctx context.Context, tx Tx, a *influxdb.Authorization) error {
	err := s.unique(ctx, tx, authIndex, authIndexKey(a.Token))
	if err == NotUniqueError {
		// by returning a generic error we are trying to hide when
		// a token is non-unique.
		return influxdb.ErrUnableToCreateToken
	}
	// otherwise, this is some sort of internal server error and we
	// should provide some debugging information.
	return err
}
