package errors

import (
	"fmt"
)

const MinPasswordLen int = 8
const MaxPasswordLen = 72
const SpecialChars = `!@#$%^&*()_+`

var (
	// ErrUserNotFound is used when the user is not found.
	ErrUserNotFound = &Error{
		Msg:  "user not found",
		Code: ENotFound,
	}

	// EIncorrectPassword is returned when any password operation fails in which
	// we do not want to leak information.
	EIncorrectPassword = &Error{
		Code: EForbidden,
		Msg:  "your username or password is incorrect",
	}

	// EIncorrectUser is returned when any user is failed to be found which indicates
	// the userID provided is for a user that does not exist.
	EIncorrectUser = &Error{
		Code: EForbidden,
		Msg:  "your userID is incorrect",
	}

	// EPasswordLength is used when a password is less than the minimum
	// acceptable password length or longer than the maximum acceptable password length
	EPasswordLength = &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("passwords must be between %d and %d characters long", MinPasswordLen, MaxPasswordLen),
	}

	EPasswordChars = &Error{
		Code: EInvalid,
		Msg: fmt.Sprintf(
			"passwords must contain at least three of the following character types: uppercase, lowercase, numbers, and special characters: %s",
			SpecialChars),
	}

	EPasswordChangeRequired = &Error{
		Code: EForbidden,
		Msg:  "password change required",
	}
)

// UserAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func UserAlreadyExistsError(n string) *Error {
	return &Error{
		Code: EConflict,
		Msg:  fmt.Sprintf("user with name %s already exists", n),
	}
}

// UserIDAlreadyExistsError is used when attempting to create a user with an ID
// that already exists.
func UserIDAlreadyExistsError(id string) *Error {
	return &Error{
		Code: EConflict,
		Msg:  fmt.Sprintf("user with ID %s already exists", id),
	}
}

// UnexpectedUserBucketError is used when the error comes from an internal system.
func UnexpectedUserBucketError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user bucket; Err: %v", err),
		Op:   "kv/userBucket",
	}
}

// UnexpectedUserIndexError is used when the error comes from an internal system.
func UnexpectedUserIndexError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user index; Err: %v", err),
		Op:   "kv/userIndex",
	}
}

// InvalidUserIDError is used when a service was provided an invalid ID.
// This is some sort of internal server error.
func InvalidUserIDError(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  "user id provided is invalid",
		Err:  err,
	}
}

// ErrCorruptUser is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptUser(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalUser",
	}
}

// ErrUnprocessableUser is used when a user is not able to be processed.
func ErrUnprocessableUser(err error) *Error {
	return &Error{
		Code: EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalUser",
	}
}

// UnavailablePasswordServiceError is used if we aren't able to add the
// password to the store, it means the store is not available at the moment
// (e.g. network).
func UnavailablePasswordServiceError(err error) *Error {
	return &Error{
		Code: EUnavailable,
		Msg:  fmt.Sprintf("Unable to connect to password service. Please try again; Err: %v", err),
		Op:   "kv/setPassword",
	}
}
