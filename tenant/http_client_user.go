package tenant

import (
	"context"
	"net/http"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	khttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// UserService connects to Influx via HTTP using tokens to manage users
type UserClientService struct {
	Client *httpc.Client
	// OpPrefix is the ops of not found error.
	OpPrefix string
}

// FindMe returns user information about the owner of the token
func (s *UserClientService) FindMe(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	var res UserResponse
	err := s.Client.
		Get(prefixMe).
		DecodeJSON(&res).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &res.User, nil
}

// FindUserByID returns a single user by ID.
func (s *UserClientService) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	var res UserResponse
	err := s.Client.
		Get(prefixUsers, id.String()).
		DecodeJSON(&res).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &res.User, nil
}

// FindUser returns the first user that matches filter.
func (s *UserClientService) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	if filter.ID == nil && filter.Name == nil {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Msg:  "user not found",
		}
	}
	users, n, err := s.FindUsers(ctx, filter)
	if err != nil {
		return nil, &errors.Error{
			Op:  s.OpPrefix + influxdb.OpFindUser,
			Err: err,
		}
	}

	if n == 0 {
		return nil, &errors.Error{
			Code: errors.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindUser,
			Msg:  "no results found",
		}
	}

	return users[0], nil
}

// FindUsers returns a list of users that match filter and the total count of matching users.
// Additional options provide pagination & sorting.
func (s *UserClientService) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	params := influxdb.FindOptionParams(opt...)
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.Name != nil {
		params = append(params, [2]string{"name", *filter.Name})
	}

	var r usersResponse
	err := s.Client.
		Get(prefixUsers).
		QueryParams(params...).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	us := r.ToInfluxdb()
	return us, len(us), nil
}

// CreateUser creates a new user and sets u.ID with the new identifier.
func (s *UserClientService) CreateUser(ctx context.Context, u *influxdb.User) error {
	return s.Client.
		PostJSON(u, prefixUsers).
		DecodeJSON(u).
		Do(ctx)
}

// UpdateUser updates a single user with changeset.
// Returns the new user state after update.
func (s *UserClientService) UpdateUser(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	var res UserResponse
	err := s.Client.
		PatchJSON(upd, prefixUsers, id.String()).
		DecodeJSON(&res).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &res.User, nil
}

// DeleteUser removes a user by ID.
func (s *UserClientService) DeleteUser(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(prefixUsers, id.String()).
		StatusFn(func(resp *http.Response) error {
			return khttp.CheckErrorStatus(http.StatusNoContent, resp)
		}).
		Do(ctx)
}

// FindUserByID returns a single user by ID.
func (s *UserClientService) FindPermissionForUser(ctx context.Context, id platform.ID) (influxdb.PermissionSet, error) {
	var ps influxdb.PermissionSet
	err := s.Client.
		Get(prefixUsers, id.String(), "permissions").
		DecodeJSON(&ps).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return ps, nil
}

// PasswordClientService is an http client to speak to the password service.
type PasswordClientService struct {
	Client *httpc.Client
}

var _ influxdb.PasswordsService = (*PasswordClientService)(nil)

// SetPassword sets the user's password.
func (s *PasswordClientService) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	return s.Client.
		PostJSON(passwordSetRequest{
			Password: password,
		}, prefixUsers, userID.String(), "password").
		Do(ctx)
}

// ComparePassword compares the user new password with existing. Note: is not implemented.
func (s *PasswordClientService) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	panic("not implemented")
}

// CompareAndSetPassword compares the old and new password and submits the new password if possible.
// Note: is not implemented.
func (s *PasswordClientService) CompareAndSetPassword(ctx context.Context, userID platform.ID, old string, new string) error {
	panic("not implemented")
}
