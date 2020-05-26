package tenant

import (
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

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
