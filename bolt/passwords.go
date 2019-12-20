package bolt

import (
	"context"
	"fmt"

	bolt "github.com/coreos/bbolt"
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

// CorruptUserIDError is used when the ID was encoded incorrectly previously.
// This is some sort of internal server error.
func CorruptUserIDError(name string, err error) error {
	return &platform.Error{
		Code: platform.EInternal,
		Msg:  fmt.Sprintf("User ID for %s has been corrupted; Err: %v", name, err),
		Op:   "bolt/setPassword",
	}
}

var _ platform.PasswordsService = (*Client)(nil)

// SetPassword stores the password hash associated with a user.
func (c *Client) SetPassword(ctx context.Context, userID platform.ID, password string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		return c.setPassword(ctx, tx, userID, password)
	})
}

// HashCost currently using the default cost of bcrypt
var HashCost = bcrypt.DefaultCost

func (c *Client) setPassword(ctx context.Context, tx *bolt.Tx, userID platform.ID, password string) error {
	if len(password) < MinPasswordLength {
		return EShortPassword
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), HashCost)
	if err != nil {
		return err
	}

	encodedID, err := userID.Encode()
	if err != nil {
		return CorruptUserIDError(userID.String(), err)
	}

	if _, err := c.findUserByID(ctx, tx, userID); err != nil {
		return EIncorrectUser
	}

	return tx.Bucket(userpasswordBucket).Put(encodedID, hash)
}

// ComparePassword compares a provided password with the stored password hash.
func (c *Client) ComparePassword(ctx context.Context, userID platform.ID, password string) error {
	return c.db.View(func(tx *bolt.Tx) error {
		return c.comparePassword(ctx, tx, userID, password)
	})
}

func (c *Client) comparePassword(ctx context.Context, tx *bolt.Tx, userID platform.ID, password string) error {
	encodedID, err := userID.Encode()
	if err != nil {
		return err
	}

	if _, err := c.findUserByID(ctx, tx, userID); err != nil {
		return EIncorrectUser
	}

	hash := tx.Bucket(userpasswordBucket).Get(encodedID)

	if err := bcrypt.CompareHashAndPassword(hash, []byte(password)); err != nil {
		// User exists but the password was incorrect
		return EIncorrectPassword
	}
	return nil
}

// CompareAndSetPassword replaces the old password with the new password if thee old password is correct.
func (c *Client) CompareAndSetPassword(ctx context.Context, userID platform.ID, old string, new string) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := c.comparePassword(ctx, tx, userID, old); err != nil {
			return err
		}
		return c.setPassword(ctx, tx, userID, new)
	})
}
