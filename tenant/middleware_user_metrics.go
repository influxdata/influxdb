package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/metric"
	"github.com/prometheus/client_golang/prometheus"
)

var _ influxdb.UserService = (*UserMetrics)(nil)
var _ influxdb.PasswordsService = (*PasswordMetrics)(nil)

type UserMetrics struct {
	// RED metrics
	rec *metric.REDClient

	userService influxdb.UserService
}

// NewUserMetrics returns a metrics service middleware for the User Service.
func NewUserMetrics(reg prometheus.Registerer, s influxdb.UserService, opts ...metric.ClientOptFn) *UserMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &UserMetrics{
		rec:         metric.New(reg, o.ApplySuffix("user")),
		userService: s,
	}
}

func (m *UserMetrics) FindUserByID(ctx context.Context, id platform.ID) (*influxdb.User, error) {
	rec := m.rec.Record("find_user_by_id")
	user, err := m.userService.FindUserByID(ctx, id)
	return user, rec(err)
}

func (m *UserMetrics) FindUser(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
	rec := m.rec.Record("find_user")
	user, err := m.userService.FindUser(ctx, filter)
	return user, rec(err)
}

func (m *UserMetrics) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
	rec := m.rec.Record("find_users")
	users, n, err := m.userService.FindUsers(ctx, filter, opt...)
	return users, n, rec(err)
}

func (m *UserMetrics) CreateUser(ctx context.Context, u *influxdb.User) error {
	rec := m.rec.Record("create_user")
	err := m.userService.CreateUser(ctx, u)
	return rec(err)
}

func (m *UserMetrics) UpdateUser(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
	rec := m.rec.Record("update_user")
	updatedUser, err := m.userService.UpdateUser(ctx, id, upd)
	return updatedUser, rec(err)
}

func (m *UserMetrics) DeleteUser(ctx context.Context, id platform.ID) error {
	rec := m.rec.Record("delete_user")
	err := m.userService.DeleteUser(ctx, id)
	return rec(err)
}

func (m *UserMetrics) FindPermissionForUser(ctx context.Context, id platform.ID) (influxdb.PermissionSet, error) {
	rec := m.rec.Record("find_permission_for_user")
	ps, err := m.userService.FindPermissionForUser(ctx, id)
	return ps, rec(err)
}

type PasswordMetrics struct {
	// RED metrics
	rec *metric.REDClient

	pwdService influxdb.PasswordsService
}

// NewPasswordMetrics returns a metrics service middleware for the Password Service.
func NewPasswordMetrics(reg prometheus.Registerer, s influxdb.PasswordsService, opts ...metric.ClientOptFn) *PasswordMetrics {
	o := metric.ApplyMetricOpts(opts...)
	return &PasswordMetrics{
		rec:        metric.New(reg, o.ApplySuffix("password")),
		pwdService: s,
	}
}

func (m *PasswordMetrics) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	rec := m.rec.Record("set_password")
	err := m.pwdService.SetPassword(ctx, userID, password)
	return rec(err)
}

func (m *PasswordMetrics) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	rec := m.rec.Record("compare_password")
	err := m.pwdService.ComparePassword(ctx, userID, password)
	return rec(err)
}

func (m *PasswordMetrics) CompareAndSetPassword(ctx context.Context, userID platform.ID, old, new string) error {
	rec := m.rec.Record("compare_and_set_password")
	err := m.pwdService.CompareAndSetPassword(ctx, userID, old, new)
	return rec(err)
}
