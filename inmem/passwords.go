package inmem

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
	"golang.org/x/crypto/bcrypt"
)

// MinPasswordLength is the shortest password we allow into the system.
const MinPasswordLength = 8

var (
	// EIncorrectPassword is returned when any password operation fails in which
	// we do not want to leak information.
	EIncorrectPassword = &platform.Error{
		Msg: "your username or password is incorrect",
	}

	// EShortPassword is used when a password is less than the minimum
	// acceptable password length.
	EShortPassword = &platform.Error{
		Msg: "passwords are required to be longer than 8 characters",
	}
)

var _ platform.PasswordsService = (*Service)(nil)

// HashCost is currently using bcrypt defaultCost
const HashCost = bcrypt.DefaultCost

// SetPassword stores the password hash associated with a user.
func (s *Service) SetPassword(ctx context.Context, name string, password string) error {
	if len(password) < MinPasswordLength {
		return EShortPassword
	}

	u, err := s.FindUser(ctx, platform.UserFilter{Name: &name})
	if err != nil {
		return EIncorrectPassword
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), HashCost)
	if err != nil {
		return err
	}

	s.basicAuthKV.Store(u.ID.String(), hash)

	return nil
}

// ComparePassword compares a provided password with the stored password hash.
func (s *Service) ComparePassword(ctx context.Context, name string, password string) error {
	u, err := s.FindUser(ctx, platform.UserFilter{Name: &name})
	if err != nil {
		return EIncorrectPassword
	}
	hash, ok := s.basicAuthKV.Load(u.ID.String())
	if !ok {
		hash = []byte{}
	}

	if err := bcrypt.CompareHashAndPassword(hash.([]byte), []byte(password)); err != nil {
		return fmt.Errorf("your username or password is incorrect")
	}
	return nil
}

// CompareAndSetPassword replaces the old password with the new password if thee old password is correct.
func (s *Service) CompareAndSetPassword(ctx context.Context, name string, old string, new string) error {
	if err := s.ComparePassword(ctx, name, old); err != nil {
		return err
	}
	return s.SetPassword(ctx, name, new)
}
