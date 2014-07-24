package coordinator

import (
	"github.com/influxdb/influxdb/common"
)

type Permissions struct{}

func (self *Permissions) AuthorizeDeleteQuery(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeDropSeries(user common.User, db string, seriesName string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to drop series")
	}

	return true, ""
}

func (self *Permissions) AuthorizeCreateContinuousQuery(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to create continuous query")
	}

	return true, ""
}

func (self *Permissions) AuthorizeDeleteContinuousQuery(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to delete continuous query")
	}

	return true, ""
}

func (self *Permissions) AuthorizeListContinuousQueries(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to list continuous queries")
	}

	return true, ""
}

func (self *Permissions) AuthorizeCreateDatabase(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to create database")
	}

	return true, ""
}

func (self *Permissions) AuthorizeListDatabases(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to list databases")
	}

	return true, ""
}

func (self *Permissions) AuthorizeDropDatabase(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to drop database")
	}

	return true, ""
}

func (self *Permissions) AuthorizeListClusterAdmins(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to list cluster admins")
	}

	return true, ""
}

func (self *Permissions) AuthorizeCreateClusterAdmin(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to create cluster admin")
	}

	return true, ""
}

func (self *Permissions) AuthorizeDeleteClusterAdmin(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to delete cluster admin")
	}

	return true, ""
}

func (self *Permissions) AuthorizeChangeClusterAdminPassword(user common.User) (ok bool, err common.AuthorizationError) {
	if !user.IsClusterAdmin() {
		return false, common.NewAuthorizationError("Insufficient permissions to change cluster admin password")
	}

	return true, ""
}

func (self *Permissions) AuthorizeCreateDbUser(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to create db user on %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeDeleteDbUser(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to delete db user on %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeListDbUsers(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to list db users on %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeGetDbUser(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to get db user on %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeChangeDbUserPassword(user common.User, db string, targetUsername string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) && !(user.GetDb() == db && user.GetName() == targetUsername) {
		return false, common.NewAuthorizationError("Insufficient permissions to change db user password for %s on %s", targetUsername, db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeChangeDbUserPermissions(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to change db user permissions on %s", db)
	}

	return true, ""
}

func (self *Permissions) AuthorizeGrantDbUserAdmin(user common.User, db string) (ok bool, err common.AuthorizationError) {
	if !user.IsDbAdmin(db) {
		return false, common.NewAuthorizationError("Insufficient permissions to grant db user admin privileges on %s", db)
	}

	return true, ""
}
