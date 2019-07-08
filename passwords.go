package influxdb

import "context"

// PasswordsService is the service for managing basic auth passwords.
type PasswordsService interface {
	// SetPassword overrides the password of a known user.
	SetPassword(ctx context.Context, name string, password string) error
	// ComparePassword checks if the password matches the password recorded.
	// Passwords that do not match return errors.
	ComparePassword(ctx context.Context, name string, password string) error
	// CompareAndSetPassword checks the password and if they match
	// updates to the new password.
	CompareAndSetPassword(ctx context.Context, name string, old string, new string) error
}
