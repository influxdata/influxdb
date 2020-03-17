package tenant

import (
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/metric"
	"github.com/influxdata/influxdb/kit/prom"
)

type UserMetrics struct {
	// RED metrics
	rec *metric.REDClient

	userService influxdb.UserService
}

var _ influxdb.UserService = (*UserMetrics)(nil)

// NewUserMetrics returns a metrics service middleware for the User Service.
func NewUserMetrics(reg *prom.Registry, s influxdb.UserService) *UserMetrics {
	return &UserMetrics{
		rec:         metric.New(reg, "user"),
		userService: s,
	}
}

// Returns a single user by ID.
func (m *UserMetrics) FindUserByID(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
	rec := m.rec.Record("find_user_by_id")
	user, err := m.userService.FindUserByID(ctx, id)
	return user, rec(err)
}

// Returns the first user that matches filter.
func (m *UserMetrics) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	rec := m.rec.Record("find_user")
	user, err := m.userService.FindUser(ctx, filter)
	return user, rec(err)
}

// Returns a list of users that match filter and the total count of matching users.
// Additional options provide pagination & sorting.
func (m *UserMetrics) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	rec := m.rec.Record("find_users")
	users, n, err := m.userService.FindUsers(ctx, filter, opt...)
	return users, n, rec(err)
}

// Creates a new user and sets u.ID with the new identifier.
func (m *UserMetrics) CreateUser(ctx context.Context, u *influxdb.User) error {
	rec := m.rec.Record("create_user")
	err := m.userService.CreateUser(ctx, u)
	return rec(err)
}

// Updates a single user with changeset.
// Returns the new user state after update.
func (m *UserMetrics) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	rec := m.rec.Record("update_user")
	updatedUser, err := m.userService.UpdateUser(ctx, id, upd)
	return updatedUser, rec(err)
}

// Removes a user by ID.
func (m *UserMetrics) DeleteUser(ctx context.Context, id influxdb.ID) error {
	rec := m.rec.Record("delete_user")
	err := m.userService.DeleteUser(ctx, id)
	return rec(err)
}
