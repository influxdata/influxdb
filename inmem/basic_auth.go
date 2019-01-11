package inmem

import (
	"context"

	platform "github.com/influxdata/influxdb"
	"golang.org/x/crypto/bcrypt"
)

// HashCost is currently using bcrypt defaultCost
const HashCost = bcrypt.DefaultCost

// SetPassword stores the password hash associated with a user.
func (s *Service) SetPassword(ctx context.Context, name string, password string) error {
	u, err := s.FindUser(ctx, platform.UserFilter{Name: &name})
	if err != nil {
		return err
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
		return err
	}
	hash, ok := s.basicAuthKV.Load(u.ID.String())
	if !ok {
		hash = []byte{}
	}

	return bcrypt.CompareHashAndPassword(hash.([]byte), []byte(password))
}

// CompareAndSetPassword replaces the old password with the new password if thee old password is correct.
func (s *Service) CompareAndSetPassword(ctx context.Context, name string, old string, new string) error {
	if err := s.ComparePassword(ctx, name, old); err != nil {
		return err
	}
	return s.SetPassword(ctx, name, new)
}
