package inmem

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	"golang.org/x/crypto/bcrypt"
)

// MinPasswordLength is the shortest password we allow into the system.
const MinPasswordLength = 8

var (
	// EIncorrectPassword is returned when any password operation fails in which
	// we do not want to leak information.
	EIncorrectPassword = &platform.Error{
		Code: platform.EForbidden,
		Msg:  "your username or password is incorrect",
	}

	// EIncorrectUser is returned when any user is failed to be found which indicates
	// the userID provided is for a user that does not exist.
	EIncorrectUser = &platform.Error{
		Code: platform.EForbidden,
		Msg:  "your userID is incorrect",
	}

	// EShortPassword is used when a password is less than the minimum
	// acceptable password length.
	EShortPassword = &platform.Error{
		Code: platform.EInvalid,
		Msg:  "passwords must be at least 8 characters long",
	}
)

var _ platform.PasswordsService = (*Service)(nil)

// HashCost is currently using bcrypt defaultCost
const HashCost = bcrypt.DefaultCost

// SetPassword stores the password hash associated with a user.
func (s *Service) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	if len(password) < MinPasswordLength {
		return EShortPassword
	}

	u, err := s.FindUserByID(ctx, userID)
	if err != nil {
		return EIncorrectUser
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), HashCost)
	if err != nil {
		return err
	}

	s.basicAuthKV.Store(u.ID.String(), hash)

	return nil
}

// ComparePassword compares a provided password with the stored password hash.
func (s *Service) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	u, err := s.FindUserByID(ctx, userID)
	if err != nil {
		return EIncorrectUser
	}
	hash, ok := s.basicAuthKV.Load(u.ID.String())
	if !ok {
		hash = []byte{}
	}

	if err := bcrypt.CompareHashAndPassword(hash.([]byte), []byte(password)); err != nil {
		return EIncorrectPassword
	}
	return nil
}

// CompareAndSetPassword replaces the old password with the new password if thee old password is correct.
func (s *Service) CompareAndSetPassword(ctx context.Context, userID platform.ID, old string, new string) error {
	if err := s.ComparePassword(ctx, userID, old); err != nil {
		return err
	}
	return s.SetPassword(ctx, userID, new)
}
