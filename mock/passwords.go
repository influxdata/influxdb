package mock

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// PasswordsService is a mock implementation of a retention.PasswordsService, which
// also makes it a suitable mock to use wherever an platform.PasswordsService is required.
type PasswordsService struct {
	SetPasswordFn           func(context.Context, platform.ID, string) error
	ComparePasswordFn       func(context.Context, platform.ID, string) error
	CompareAndSetPasswordFn func(context.Context, platform.ID, string, string) error
}

// NewPasswordsService returns a mock PasswordsService where its methods will return
// zero values.
func NewPasswordsService() *PasswordsService {
	return &PasswordsService{
		SetPasswordFn:           func(context.Context, platform.ID, string) error { return fmt.Errorf("mock error") },
		ComparePasswordFn:       func(context.Context, platform.ID, string) error { return fmt.Errorf("mock error") },
		CompareAndSetPasswordFn: func(context.Context, platform.ID, string, string) error { return fmt.Errorf("mock error") },
	}
}

// SetPassword sets the users current password to be the provided password.
func (s *PasswordsService) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	return s.SetPasswordFn(ctx, userID, password)
}

// ComparePassword password compares the provided password.
func (s *PasswordsService) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	return s.ComparePasswordFn(ctx, userID, password)
}

// CompareAndSetPassword compares the provided password and sets it to the new password.
func (s *PasswordsService) CompareAndSetPassword(ctx context.Context, userID platform.ID, old string, new string) error {
	return s.CompareAndSetPasswordFn(ctx, userID, old, new)
}
