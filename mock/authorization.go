package mock

import (
	influxdb "github.com/influxdata/influxdb/v2"
)

// ensure Authorizer implements influxdb.Authorizer
var _ influxdb.Authorizer = (*Authorizer)(nil)

// Authorizer is an Authorizer for testing that can allow everything or use specific permissions
type Authorizer struct {
	Permissions []influxdb.Permission
	AllowAll    bool
	UserID      influxdb.ID
}

func NewMockAuthorizer(allowAll bool, permissions []influxdb.Permission) *Authorizer {
	if allowAll {
		return &Authorizer{
			AllowAll: true,
		}
	}
	return &Authorizer{
		AllowAll:    false,
		Permissions: permissions,
	}
}

func (a *Authorizer) PermissionSet() (influxdb.PermissionSet, error) {
	if a.AllowAll {
		return influxdb.OperPermissions(), nil
	}

	return a.Permissions, nil
}

func (a *Authorizer) Identifier() influxdb.ID {
	return 1
}

func (a *Authorizer) GetUserID() influxdb.ID {
	if a.UserID.Valid() {
		return a.UserID
	}

	return 2
}

func (Authorizer) Kind() string {
	return "mock"
}
