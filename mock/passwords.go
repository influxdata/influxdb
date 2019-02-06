package mock

import (
	"context"
	"fmt"
)

// BasicAuthService is a mock implementation of a retention.BasicAuthService, which
// also makes it a suitable mock to use wherever an platform.BasicAuthService is required.
type BasicAuthService struct {
	SetPasswordFn           func(context.Context, string, string) error
	ComparePasswordFn       func(context.Context, string, string) error
	CompareAndSetPasswordFn func(context.Context, string, string, string) error
}

// NewBasicAuthService returns a mock BasicAuthService where its methods will return
// zero values.
func NewBasicAuthService(user, password string) *BasicAuthService {
	return &BasicAuthService{
		SetPasswordFn:           func(context.Context, string, string) error { return fmt.Errorf("mock error") },
		ComparePasswordFn:       func(context.Context, string, string) error { return fmt.Errorf("mock error") },
		CompareAndSetPasswordFn: func(context.Context, string, string, string) error { return fmt.Errorf("mock error") },
	}
}

// SetPassword sets the users current password to be the provided password.
func (s *BasicAuthService) SetPassword(ctx context.Context, name string, password string) error {
	return s.SetPasswordFn(ctx, name, password)
}

// ComparePassword password compares the provided password.
func (s *BasicAuthService) ComparePassword(ctx context.Context, name string, password string) error {
	return s.ComparePasswordFn(ctx, name, password)
}

// CompareAndSetPassword compares the provided password and sets it to the new password.
func (s *BasicAuthService) CompareAndSetPassword(ctx context.Context, name string, old string, new string) error {
	return s.CompareAndSetPasswordFn(ctx, name, old, new)
}
