package coordinator

import (
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	. "launchpad.net/gocheck"
)

type PermissionsSuite struct {
	permissions       Permissions
	commonUser        *cluster.DbUser
	commonUserNoWrite *cluster.DbUser
	dbAdmin           *cluster.DbUser
	clusterAdmin      *cluster.ClusterAdmin
}

var _ = Suite(&PermissionsSuite{})

func (self *PermissionsSuite) SetUpSuite(c *C) {
	self.permissions = Permissions{}
	self.dbAdmin = &cluster.DbUser{cluster.CommonUser{"db_admin", "", false, "db_admin"}, "db", nil, nil, true}
	self.commonUser = &cluster.DbUser{cluster.CommonUser{"common_user", "", false, "common_user"}, "db", []*cluster.Matcher{{true, ".*"}}, []*cluster.Matcher{{true, ".*"}}, false}
	self.commonUserNoWrite = &cluster.DbUser{cluster.CommonUser{"no_write_user", "", false, "no_write_user"}, "db", nil, nil, false}
	self.clusterAdmin = &cluster.ClusterAdmin{cluster.CommonUser{"root", "", false, "root"}}
}

func (self *PermissionsSuite) TestAuthorizeDeleteQuery(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permission to write to db")

	ok, err = self.permissions.AuthorizeDeleteQuery(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDeleteQuery(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeDeleteQuery(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeDropSeries(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to drop series")

	ok, _ = self.permissions.AuthorizeDropSeries(self.dbAdmin, "db", "series")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeDropSeries(self.clusterAdmin, "db", "series")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeDropSeries(self.commonUser, "db", "series")
	c.Assert(ok, Equals, false)

	ok, err = self.permissions.AuthorizeDropSeries(self.commonUserNoWrite, "db", "series")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)
}

func (self *PermissionsSuite) TestAuthorizeCreateContinuousQuery(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to create continuous query")

	ok, err = self.permissions.AuthorizeCreateContinuousQuery(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateContinuousQuery(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeCreateContinuousQuery(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeDeleteContinuousQuery(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to delete continuous query")

	ok, err = self.permissions.AuthorizeDeleteContinuousQuery(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDeleteContinuousQuery(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeDeleteContinuousQuery(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeListContinuousQueries(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to list continuous queries")

	ok, err = self.permissions.AuthorizeListContinuousQueries(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListContinuousQueries(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeListContinuousQueries(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeCreateDatabase(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to create database")

	ok, err = self.permissions.AuthorizeCreateDatabase(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateDatabase(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateDatabase(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeListDatabases(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to list databases")

	ok, err = self.permissions.AuthorizeListDatabases(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListDatabases(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListDatabases(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeDropDatabase(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to drop database")

	ok, err = self.permissions.AuthorizeDropDatabase(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDropDatabase(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDropDatabase(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeListClusterAdmins(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to list cluster admins")

	ok, err = self.permissions.AuthorizeListClusterAdmins(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListClusterAdmins(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListClusterAdmins(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeCreateClusterAdmin(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to create cluster admin")

	ok, err = self.permissions.AuthorizeCreateClusterAdmin(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateClusterAdmin(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateClusterAdmin(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeDeleteClusterAdmin(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to delete cluster admin")

	ok, err = self.permissions.AuthorizeDeleteClusterAdmin(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDeleteClusterAdmin(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDeleteClusterAdmin(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeChangeClusterAdminPassword(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to change cluster admin password")

	ok, err = self.permissions.AuthorizeChangeClusterAdminPassword(self.commonUser)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeChangeClusterAdminPassword(self.dbAdmin)
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeChangeClusterAdminPassword(self.clusterAdmin)
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeCreateDbUser(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to create db user on db")

	ok, err = self.permissions.AuthorizeCreateDbUser(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeCreateDbUser(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeCreateDbUser(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeDeleteDbUser(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to delete db user on db")

	ok, err = self.permissions.AuthorizeDeleteDbUser(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeDeleteDbUser(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeDeleteDbUser(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeListDbUsers(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to list db users on db")

	ok, err = self.permissions.AuthorizeListDbUsers(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeListDbUsers(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeListDbUsers(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeGetDbUser(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to get db user on db")

	ok, err = self.permissions.AuthorizeGetDbUser(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeGetDbUser(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeGetDbUser(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeChangeDbUserPassword(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to change db user password for someone on db")

	ok, err = self.permissions.AuthorizeChangeDbUserPassword(self.commonUser, "db", "someone")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeChangeDbUserPassword(self.dbAdmin, "db", "someone")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeChangeDbUserPassword(self.clusterAdmin, "db", "someone")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeChangeDbUserPassword(self.commonUser, "db", "common_user")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeChangeDbUserPassword(self.commonUser, "db", "no_write_user")
	c.Assert(ok, Equals, false)
}

func (self *PermissionsSuite) TestAuthorizeChangeDbUserPermissions(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to change db user permissions on db")

	ok, err = self.permissions.AuthorizeChangeDbUserPermissions(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeChangeDbUserPermissions(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeChangeDbUserPermissions(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}

func (self *PermissionsSuite) TestAuthorizeGrantDbUserAdmin(c *C) {
	var ok bool
	var err common.AuthorizationError

	authErr := common.NewAuthorizationError("Insufficient permissions to grant db user admin privileges on db")

	ok, err = self.permissions.AuthorizeGrantDbUserAdmin(self.commonUser, "db")
	c.Assert(ok, Equals, false)
	c.Assert(err, Equals, authErr)

	ok, _ = self.permissions.AuthorizeGrantDbUserAdmin(self.dbAdmin, "db")
	c.Assert(ok, Equals, true)

	ok, _ = self.permissions.AuthorizeGrantDbUserAdmin(self.clusterAdmin, "db")
	c.Assert(ok, Equals, true)
}
