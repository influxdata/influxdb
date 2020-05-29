package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

var _ influxdb.UserService = (*UserLogger)(nil)
var _ influxdb.PasswordsService = (*PasswordLogger)(nil)

type UserLogger struct {
	logger      *zap.Logger
	userService influxdb.UserService
}

// NewUserLogger returns a logging service middleware for the User Service.
func NewUserLogger(log *zap.Logger, s influxdb.UserService) *UserLogger {
	return &UserLogger{
		logger:      log,
		userService: s,
	}
}

func (l *UserLogger) CreateUser(ctx context.Context, u *influxdb.User) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to create user", zap.Error(err), dur)
			return
		}
		l.logger.Debug("user create", dur)
	}(time.Now())
	return l.userService.CreateUser(ctx, u)
}

func (l *UserLogger) FindUserByID(ctx context.Context, id influxdb.ID) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to find user with ID %v", id)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("user find by ID", dur)
	}(time.Now())
	return l.userService.FindUserByID(ctx, id)
}

func (l *UserLogger) FindUser(ctx context.Context, filter influxdb.UserFilter) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find user matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("user find", dur)
	}(time.Now())
	return l.userService.FindUser(ctx, filter)
}

func (l *UserLogger) FindUsers(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) (users []*influxdb.User, n int, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to find users matching the given filter", zap.Error(err), dur)
			return
		}
		l.logger.Debug("users find", dur)
	}(time.Now())
	return l.userService.FindUsers(ctx, filter, opt...)
}

func (l *UserLogger) UpdateUser(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (u *influxdb.User, err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			l.logger.Debug("failed to update user", zap.Error(err), dur)
			return
		}
		l.logger.Debug("user update", dur)
	}(time.Now())
	return l.userService.UpdateUser(ctx, id, upd)
}

func (l *UserLogger) DeleteUser(ctx context.Context, id influxdb.ID) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to delete user with ID %v", id)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("user create", dur)
	}(time.Now())
	return l.userService.DeleteUser(ctx, id)
}

type PasswordLogger struct {
	logger     *zap.Logger
	pwdService influxdb.PasswordsService
}

// NewPasswordLogger returns a logging service middleware for the Password Service.
func NewPasswordLogger(log *zap.Logger, s influxdb.PasswordsService) *PasswordLogger {
	return &PasswordLogger{
		logger:     log,
		pwdService: s,
	}
}

func (l *PasswordLogger) SetPassword(ctx context.Context, userID influxdb.ID, password string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to set password for user with ID %v", userID)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("set password", dur)
	}(time.Now())
	return l.pwdService.SetPassword(ctx, userID, password)
}

func (l *PasswordLogger) ComparePassword(ctx context.Context, userID influxdb.ID, password string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to compare password for user with ID %v", userID)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("compare password", dur)
	}(time.Now())
	return l.pwdService.ComparePassword(ctx, userID, password)
}

func (l *PasswordLogger) CompareAndSetPassword(ctx context.Context, userID influxdb.ID, old, new string) (err error) {
	defer func(start time.Time) {
		dur := zap.Duration("took", time.Since(start))
		if err != nil {
			msg := fmt.Sprintf("failed to compare and set password for user with ID %v", userID)
			l.logger.Debug(msg, zap.Error(err), dur)
			return
		}
		l.logger.Debug("compare and set password", dur)
	}(time.Now())
	return l.pwdService.CompareAndSetPassword(ctx, userID, old, new)
}
