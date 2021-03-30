package tenant

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

const MinPasswordLen int = 8

var (
	// ErrUserNotFound is used when the user is not found.
	ErrUserNotFound = &errors.Error{
		Msg:  "user not found",
		Code: errors.ENotFound,
	}

	// EIncorrectPassword is returned when any password operation fails in which
	// we do not want to leak information.
	EIncorrectPassword = &errors.Error{
		Code: errors.EForbidden,
		Msg:  "your username or password is incorrect",
	}

	// EIncorrectUser is returned when any user is failed to be found which indicates
	// the userID provided is for a user that does not exist.
	EIncorrectUser = &errors.Error{
		Code: errors.EForbidden,
		Msg:  "your userID is incorrect",
	}

	// EShortPassword is used when a password is less than the minimum
	// acceptable password length.
	EShortPassword = &errors.Error{
		Code: errors.EInvalid,
		Msg:  fmt.Sprintf("passwords must be at least %d characters long", MinPasswordLen),
	}
)

// UserAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func UserAlreadyExistsError(n string) *errors.Error {
	return &errors.Error{
		Code: errors.EConflict,
		Msg:  fmt.Sprintf("user with name %s already exists", n),
	}
}

// UnexpectedUserBucketError is used when the error comes from an internal system.
func UnexpectedUserBucketError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user bucket; Err: %v", err),
		Op:   "kv/userBucket",
	}
}

// UnexpectedUserIndexError is used when the error comes from an internal system.
func UnexpectedUserIndexError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user index; Err: %v", err),
		Op:   "kv/userIndex",
	}
}

// InvalidUserIDError is used when a service was provided an invalid ID.
// This is some sort of internal server error.
func InvalidUserIDError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "user id provided is invalid",
		Err:  err,
	}
}

// ErrCorruptUser is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptUser(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalUser",
	}
}

// ErrUnprocessableUser is used when a user is not able to be processed.
func ErrUnprocessableUser(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalUser",
	}
}

// UnavailablePasswordServiceError is used if we aren't able to add the
// password to the store, it means the store is not available at the moment
// (e.g. network).
func UnavailablePasswordServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EUnavailable,
		Msg:  fmt.Sprintf("Unable to connect to password service. Please try again; Err: %v", err),
		Op:   "kv/setPassword",
	}
}
