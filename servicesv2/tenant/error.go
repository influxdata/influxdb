package tenant

import (
	"fmt"
	"strings"

	"github.com/influxdata/influxdb"
)

var (
	// ErrNameisEmpty is when a name is empty
	ErrNameisEmpty = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "name is empty",
	}

	// NotUniqueIDError is used when attempting to create an org or bucket that already
	// exists.
	NotUniqueIDError = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "ID already exists",
	}

	// ErrFailureGeneratingID occurs ony when the random number generator
	// cannot generate an ID in MaxIDGenerationN times.
	ErrFailureGeneratingID = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "unable to generate valid id",
	}

	// ErrOnboardingNotAllowed occurs when request to onboard comes in and we are not allowing this request
	ErrOnboardingNotAllowed = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "onboarding has already been completed",
	}

	ErrOnboardInvalid = &influxdb.Error{
		Code: influxdb.EEmptyValue,
		Msg:  "onboard failed, missing value",
	}

	ErrNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "not found",
	}
)

// ErrInternalServiceError is used when the error comes from an internal system.
func ErrInternalServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Err:  err,
	}
}

type errSlice []error

func (e errSlice) Error() string {
	l := len(e)
	sb := strings.Builder{}
	for i, err := range e {
		if i > 0 {
			sb.WriteRune('\n')
		}
		sb.WriteString(fmt.Sprintf("error %d/%d: %s", i+1, l, err.Error()))
	}
	return sb.String()
}

// AggregateError enables composing multiple errors.
// This is ideal in the case that you are applying functions with side effects to a slice of elements.
// E.g., deleting/updating a slice of resources.
type AggregateError struct {
	errs errSlice
}

// NewAggregateError returns a new AggregateError.
func NewAggregateError() *AggregateError {
	return &AggregateError{
		errs: make([]error, 0),
	}
}

// Add adds an error to the aggregate.
func (e *AggregateError) Add(err error) {
	if err == nil {
		return
	}
	e.errs = append(e.errs, err)
}

// Err returns a proper error from this aggregate error.
func (e *AggregateError) Err() error {
	if len(e.errs) > 0 {
		return e.errs
	}
	return nil
}

var (
	// ErrOrgNotFound is used when the user is not found.
	ErrOrgNotFound = &influxdb.Error{
		Msg:  "organization not found",
		Code: influxdb.ENotFound,
	}
)

// OrgAlreadyExistsError is used when creating a new organization with
// a name that has already been used. Organization names must be unique.
func OrgAlreadyExistsError(name string) error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("organization with name %s already exists", name),
	}
}

func OrgNotFoundByName(name string) error {
	return &influxdb.Error{
		Code: influxdb.ENotFound,
		Op:   influxdb.OpFindOrganizations,
		Msg:  fmt.Sprintf("organization name \"%s\" not found", name),
	}
}

// ErrCorruptOrg is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptOrg(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalOrg",
	}
}

// ErrUnprocessableOrg is used when a org is not able to be processed.
func ErrUnprocessableOrg(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalOrg",
	}
}

// InvalidOrgIDError is used when a service was provided an invalid ID.
// This is some sort of internal server error.
func InvalidOrgIDError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "org id provided is invalid",
		Err:  err,
	}
}

var (
	invalidBucketListRequest = &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "invalid bucket list action, call should be GetBucketByName",
		Op:   "kv/listBucket",
	}

	errRenameSystemBucket = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "system buckets cannot be renamed",
	}

	errDeleteSystemBucket = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "system buckets cannot be deleted",
	}

	ErrBucketNotFound = &influxdb.Error{
		Code: influxdb.ENotFound,
		Msg:  "bucket not found",
	}

	ErrBucketNameNotUnique = &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  "bucket name is not unique",
	}
)

// ErrBucketNotFoundByName is used when the user is not found.
func ErrBucketNotFoundByName(n string) *influxdb.Error {
	return &influxdb.Error{
		Msg:  fmt.Sprintf("bucket %q not found", n),
		Code: influxdb.ENotFound,
	}
}

// ErrCorruptBucket is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptBucket(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalBucket",
	}
}

// BucketAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func BucketAlreadyExistsError(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("bucket with name %s already exists", n),
	}
}

// ErrUnprocessableBucket is used when a org is not able to be processed.
func ErrUnprocessableBucket(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalBucket",
	}
}

var (
	// ErrInvalidURMID is used when the service was provided
	// an invalid ID format.
	ErrInvalidURMID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "provided user resource mapping ID has invalid format",
	}

	// ErrURMNotFound is used when the user resource mapping is not found.
	ErrURMNotFound = &influxdb.Error{
		Msg:  "user to resource mapping not found",
		Code: influxdb.ENotFound,
	}
)

// UnavailableURMServiceError is used if we aren't able to interact with the
// store, it means the store is not available at the moment (e.g. network).
func UnavailableURMServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unable to connect to resource mapping service. Please try again; Err: %v", err),
		Op:   "kv/userResourceMapping",
	}
}

// CorruptURMError is used when the config cannot be unmarshalled from the
// bytes stored in the kv.
func CorruptURMError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unknown internal user resource mapping data error; Err: %v", err),
		Op:   "kv/userResourceMapping",
	}
}

// ErrUnprocessableMapping is used when a user resource mapping  is not able to be converted to JSON.
func ErrUnprocessableMapping(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  fmt.Sprintf("unable to convert mapping of user to resource into JSON; Err %v", err),
	}
}

// NonUniqueMappingError is an internal error when a user already has
// been mapped to a resource
func NonUniqueMappingError(userID influxdb.ID) error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("Unexpected error when assigning user to a resource: mapping for user %s already exists", userID.String()),
	}
}

var (
	// ErrUserNotFound is used when the user is not found.
	ErrUserNotFound = &influxdb.Error{
		Msg:  "user not found",
		Code: influxdb.ENotFound,
	}

	// EIncorrectPassword is returned when any password operation fails in which
	// we do not want to leak information.
	EIncorrectPassword = &influxdb.Error{
		Code: influxdb.EForbidden,
		Msg:  "your username or password is incorrect",
	}

	// EIncorrectUser is returned when any user is failed to be found which indicates
	// the userID provided is for a user that does not exist.
	EIncorrectUser = &influxdb.Error{
		Code: influxdb.EForbidden,
		Msg:  "your userID is incorrect",
	}

	// EShortPassword is used when a password is less than the minimum
	// acceptable password length.
	EShortPassword = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "passwords must be at least 8 characters long",
	}
)

// UserAlreadyExistsError is used when attempting to create a user with a name
// that already exists.
func UserAlreadyExistsError(n string) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EConflict,
		Msg:  fmt.Sprintf("user with name %s already exists", n),
	}
}

// UnexpectedUserBucketError is used when the error comes from an internal system.
func UnexpectedUserBucketError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user bucket; Err: %v", err),
		Op:   "kv/userBucket",
	}
}

// UnexpectedUserIndexError is used when the error comes from an internal system.
func UnexpectedUserIndexError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving user index; Err: %v", err),
		Op:   "kv/userIndex",
	}
}

// InvalidUserIDError is used when a service was provided an invalid ID.
// This is some sort of internal server error.
func InvalidUserIDError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "user id provided is invalid",
		Err:  err,
	}
}

// ErrCorruptUser is used when the user cannot be unmarshalled from the bytes
// stored in the kv.
func ErrCorruptUser(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EInternal,
		Msg:  "user could not be unmarshalled",
		Err:  err,
		Op:   "kv/UnmarshalUser",
	}
}

// ErrUnprocessableUser is used when a user is not able to be processed.
func ErrUnprocessableUser(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnprocessableEntity,
		Msg:  "user could not be marshalled",
		Err:  err,
		Op:   "kv/MarshalUser",
	}
}

// UnavailablePasswordServiceError is used if we aren't able to add the
// password to the store, it means the store is not available at the moment
// (e.g. network).
func UnavailablePasswordServiceError(err error) *influxdb.Error {
	return &influxdb.Error{
		Code: influxdb.EUnavailable,
		Msg:  fmt.Sprintf("Unable to connect to password service. Please try again; Err: %v", err),
		Op:   "kv/setPassword",
	}
}
