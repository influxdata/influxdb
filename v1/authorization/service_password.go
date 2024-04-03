package authorization

import (
	"context"
	eBase "errors"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/tenant"
	"golang.org/x/crypto/bcrypt"
)

// SetPasswordHash updates the password hash for id. If passHash is not a valid bcrypt hash,
// SetPasswordHash returns an error.
//
// This API is intended for upgrading 1.x users.
func (s *Service) SetPasswordHash(ctx context.Context, authID platform.ID, passHash string) error {
	// verify passHash is a valid bcrypt hash
	_, err := bcrypt.Cost([]byte(passHash))
	if err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "invalid bcrypt hash",
			Err:  err,
		}
	}
	// set password
	return s.store.Update(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetAuthorizationByID(ctx, tx, authID)
		if err != nil {
			return ErrAuthNotFound
		}
		return s.store.SetPassword(ctx, tx, authID, passHash)
	})
}

// SetPassword overrides the password of a known user.
func (s *Service) SetPassword(ctx context.Context, authID platform.ID, password string) error {
	if err := tenant.IsPasswordStrong(password, s.strongPasswords); err != nil {
		return err
	}
	passHash, err := encryptPassword(password)
	if err != nil {
		return err
	}
	// set password
	return s.store.Update(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetAuthorizationByID(ctx, tx, authID)
		if err != nil {
			return ErrAuthNotFound
		}
		return s.store.SetPassword(ctx, tx, authID, passHash)
	})
}

// ComparePassword checks if the password matches the password recorded.
// Passwords that do not match return errors, as do too weak passwords
func (s *Service) ComparePassword(ctx context.Context, authID platform.ID, password string) error {
	err := s.comparePasswordNoStrengthCheck(ctx, authID, password)
	// If a password matches, but is too weak, force user to change
	if errStrength := tenant.IsPasswordStrong(password, s.strongPasswords); err == nil && errStrength != nil {
		return eBase.Join(errors.EPasswordChangeRequired, errStrength)
	}
	return err
}

func (s *Service) comparePasswordNoStrengthCheck(ctx context.Context, authID platform.ID, password string) error {
	// get password
	var hash []byte
	err := s.store.View(ctx, func(tx kv.Tx) error {
		_, err := s.store.GetAuthorizationByID(ctx, tx, authID)
		if err != nil {
			return ErrAuthNotFound
		}
		h, err := s.store.GetPassword(ctx, tx, authID)
		if err != nil {
			if err == kv.ErrKeyNotFound {
				return errors.EIncorrectPassword
			}
			return err
		}
		hash = []byte(h)
		return nil
	})
	if err != nil {
		return err
	}
	// compare password
	if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
		return errors.EIncorrectPassword
	}

	return nil
}

// CompareAndSetPassword checks the password and if they match
// updates to the new password.
func (s *Service) CompareAndSetPassword(ctx context.Context, authID platform.ID, old, new string) error {
	err := s.comparePasswordNoStrengthCheck(ctx, authID, old)
	if err != nil {
		return err
	}

	return s.SetPassword(ctx, authID, new)
}

func encryptPassword(password string) (string, error) {
	passHash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(passHash), nil
}
