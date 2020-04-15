package mock

import (
	influxdb "github.com/influxdata/influxdb/v2"
)

// Authorizer is an Authorizer for testing that can allow everything or use specific permissions
type Authorizer struct {
	Permissions []influxdb.Permission
	AllowAll    bool
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

func (a *Authorizer) Allowed(p influxdb.Permission) bool {
	if a.AllowAll {
		return true
	}
	return influxdb.PermissionAllowed(p, a.Permissions)
}

func (a *Authorizer) Identifier() influxdb.ID {
	return 1
}

func (a *Authorizer) GetUserID() influxdb.ID {
	return 2
}

func (Authorizer) Kind() string {
	return "mock"
}
