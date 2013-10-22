package coordinator

import (
	"fmt"
	. "launchpad.net/gocheck"
	"protocol"
)

type UserSuite struct{}

var _ = Suite(&UserSuite{})

var root *User

func (self *UserSuite) SetUpSuite(c *C) {
	root = CreateTestUser("root", true)
	c.Assert(root.ChangePassword(root, "password"), IsNil)
}

func (self *UserSuite) TestClusterAdminUser(c *C) {
	c.Assert(root.GetName(), Equals, "root")
	c.Assert(root.isValidPwd("password"), Equals, true)
	c.Assert(root.isValidPwd("password1"), Equals, false)

	// can create users
	u, err := root.CreateUser("new_user")
	c.Assert(err, IsNil)
	c.Assert(u, NotNil)
	c.Assert(u.GetName(), Equals, "new_user")

	// can change users' passwords
	c.Assert(root.ChangePassword(u, "new_password"), IsNil)
	c.Assert(u.isValidPwd("new_password"), Equals, true)
	c.Assert(u.isValidPwd("new_password1"), Equals, false)

	// can set cluster admin status on other users
	c.Assert(root.SetClusterAdmin(u, true), IsNil)
	c.Assert(u.IsClusterAdmin(), Equals, true)

	// can add a user as db admin
	c.Assert(root.SetDbAdmin(u, "db1"), IsNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, true)
	c.Assert(u.IsDbAdmin("db2"), Equals, false)
	c.Assert(root.RemoveDbAdmin(u, "db1"), IsNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, false)

	// can add read and write keys that are regex
	c.Assert(root.AddReadMatcher(u, protocol.NewMatcher(false, "db1")), IsNil)
	c.Assert(root.AddReadMatcher(u, protocol.NewMatcher(true, "db2.*")), IsNil)
	c.Assert(u.HasReadAccess("db1"), Equals, true)
	c.Assert(u.HasReadAccess("db2.foobar"), Equals, true)
	c.Assert(root.RemoveReadMatcher(u, protocol.NewMatcher(false, "db1")), IsNil)
	c.Assert(u.HasReadAccess("db1"), Equals, false)
}

func (self *UserSuite) TestDbAdminUser(c *C) {
	dbAdmin, err := root.CreateUser("db_admin")
	c.Assert(err, IsNil)
	c.Assert(root.SetDbAdmin(dbAdmin, "db1"), IsNil)

	// cannot create new users
	u, err := dbAdmin.CreateUser("new_user")
	c.Assert(err, NotNil)

	// but root can
	u, _ = root.CreateUser("new_user")
	root.ChangePassword(u, "some_password")

	// can change their own password
	c.Assert(dbAdmin.ChangePassword(dbAdmin, "new_password1"), IsNil)
	c.Assert(dbAdmin.isValidPwd("new_password1"), Equals, true)
	// cannot change other users's passwords
	c.Assert(u.isValidPwd("new_password"), Equals, false)
	c.Assert(dbAdmin.ChangePassword(u, "new_password"), NotNil)
	c.Assert(u.isValidPwd("new_password"), Equals, false)

	// cannot set cluster admin status on other users including self
	c.Assert(dbAdmin.SetClusterAdmin(dbAdmin, true), NotNil)
	c.Assert(dbAdmin.SetClusterAdmin(u, true), NotNil)

	// can add a user as db admin only to db that the user is admin for
	c.Assert(dbAdmin.SetDbAdmin(u, "db1"), IsNil)
	c.Assert(dbAdmin.SetDbAdmin(u, "db2"), NotNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, true)
	c.Assert(u.IsDbAdmin("db2"), Equals, false)
	c.Assert(dbAdmin.RemoveDbAdmin(u, "db1"), IsNil)
	c.Assert(u.IsDbAdmin("db1"), Equals, false)
	c.Assert(u.IsDbAdmin("db2"), Equals, false)

	// can add read access to the db that he's administering only
	c.Assert(dbAdmin.AddReadMatcher(u, protocol.NewMatcher(false, "db1")), IsNil)
	c.Assert(dbAdmin.AddReadMatcher(u, protocol.NewMatcher(false, "db2")), NotNil)
	c.Assert(dbAdmin.AddReadMatcher(u, protocol.NewMatcher(true, "db2.*")), NotNil)
	c.Assert(u.HasReadAccess("db1"), Equals, true)
	c.Assert(u.HasReadAccess("db2"), Equals, false)
	c.Assert(u.HasReadAccess("db2.foobar"), Equals, false)
}

func (self *UserSuite) BenchmarkHashing(c *C) {
	for i := 0; i < c.N; i++ {
		pwd := fmt.Sprintf("password%d", i)
		_, err := hashPassword(pwd)
		c.Assert(err, IsNil)
	}
}
