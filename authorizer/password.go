package authorizer

import (
	"context"

	"github.com/influxdata/influxdb"
)

// PasswordService is a new authorization middleware for a password service.
type PasswordService struct {
	next influxdb.PasswordsService
}

// NewPasswordService wraps an existing password service with auth middlware.
func NewPasswordService(svc influxdb.PasswordsService) *PasswordService {
	return &PasswordService{next: svc}
}

// SetPassword overrides the password of a known user.
func (s *PasswordService) SetPassword(ctx context.Context, userID influxdb.ID, password string) error {
	if _, _, err := AuthorizeWriteResource(ctx, influxdb.UsersResourceType, userID); err != nil {
		return err
	}
	return s.next.SetPassword(ctx, userID, password)
}

// ComparePassword checks if the password matches the password recorded.
// Passwords that do not match return errors.
func (s *PasswordService) ComparePassword(ctx context.Context, userID influxdb.ID, password string) error {
	panic("not implemented")
}

// CompareAndSetPassword checks the password and if they match
// updates to the new password.
func (s *PasswordService) CompareAndSetPassword(ctx context.Context, userID influxdb.ID, old string, new string) error {
	panic("not implemented")
}
