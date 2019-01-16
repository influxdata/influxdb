package authorizer_test

import "github.com/influxdata/influxdb"

// Authorizer is mock authorizer that can be used in testing.
type Authorizer struct {
	Permissions []influxdb.Permission
}

func (a *Authorizer) Allowed(p influxdb.Permission) bool {
	return influxdb.PermissionAllowed(p, a.Permissions)
}

func (a *Authorizer) Identifier() influxdb.ID {
	return 1
}

func (a *Authorizer) GetUserID() influxdb.ID {
	return 2
}

func (a *Authorizer) Kind() string {
	return "mock"
}
