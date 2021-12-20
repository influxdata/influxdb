package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// UserStatus indicates whether a user is active or inactive
type UserStatus string

// Valid validates user status
func (u *UserStatus) Valid() error {
	if *u != "active" && *u != "inactive" {
		return &errors.Error{Code: errors.EInvalid, Msg: "Invalid user status"}
	}

	return nil
}

// User is a user. 🎉
type User struct {
	ID      platform.ID `json:"id,omitempty"`
	Name    string      `json:"name"`
	OAuthID string      `json:"oauthID,omitempty"`
	Status  Status      `json:"status"`
}

// Valid validates user
func (u *User) Valid() error {
	return u.Status.Valid()
}

// Ops for user errors and op log.
const (
	OpFindUserByID = "FindUserByID"
	OpFindUser     = "FindUser"
	OpFindUsers    = "FindUsers"
	OpCreateUser   = "CreateUser"
	OpUpdateUser   = "UpdateUser"
	OpDeleteUser   = "DeleteUser"
)

// UserService represents a service for managing user data.
type UserService interface {

	// Returns a single user by ID.
	FindUserByID(ctx context.Context, id platform.ID) (*User, error)

	// Returns the first user that matches filter.
	FindUser(ctx context.Context, filter UserFilter) (*User, error)

	// Returns a list of users that match filter and the total count of matching users.
	// Additional options provide pagination & sorting.
	FindUsers(ctx context.Context, filter UserFilter, opt ...FindOptions) ([]*User, int, error)

	// Creates a new user and sets u.ID with the new identifier.
	CreateUser(ctx context.Context, u *User) error

	// Updates a single user with changeset.
	// Returns the new user state after update.
	UpdateUser(ctx context.Context, id platform.ID, upd UserUpdate) (*User, error)

	// Removes a user by ID.
	DeleteUser(ctx context.Context, id platform.ID) error

	// FindPermissionForUser
	FindPermissionForUser(ctx context.Context, UserID platform.ID) (PermissionSet, error)
}

// UserUpdate represents updates to a user.
// Only fields which are set are updated.
type UserUpdate struct {
	Name   *string `json:"name"`
	Status *Status `json:"status"`
}

// Valid validates UserUpdate
func (uu UserUpdate) Valid() error {
	if uu.Status == nil {
		return nil
	}

	return uu.Status.Valid()
}

// UserFilter represents a set of filter that restrict the returned results.
type UserFilter struct {
	ID   *platform.ID
	Name *string
}

// UserResponse is the response of user
type UserResponse struct {
	Links map[string]string `json:"links"`
	User
}
