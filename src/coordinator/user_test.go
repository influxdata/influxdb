package coordinator

import (
	. "launchpad.net/gocheck"
)

type UserSuite struct{}

var _ = Suite(&UserSuite{})

var root User

func (self *UserSuite) SetUpSuite(c *C) {
	root = &clusterAdmin{CommonUser{"root", "", false}}
	c.Assert(root.changePassword("password"), IsNil)
}

func (self *UserSuite) TestProperties(c *C) {
	u := clusterAdmin{CommonUser{Name: "root"}}
	c.Assert(u.IsClusterAdmin(), Equals, true)
	c.Assert(u.GetName(), Equals, "root")
	c.Assert(u.changePassword("foobar"), IsNil)
	c.Assert(u.isValidPwd("foobar"), Equals, true)
	c.Assert(u.isValidPwd("password"), Equals, false)

	dbUser := dbUser{CommonUser{Name: "db_user"}, "db", nil, nil, true}
	c.Assert(dbUser.IsClusterAdmin(), Equals, false)
	c.Assert(dbUser.IsDbAdmin("db"), Equals, true)
	c.Assert(dbUser.GetName(), Equals, "db_user")
	c.Assert(dbUser.changePassword("password"), IsNil)
	c.Assert(dbUser.isValidPwd("password"), Equals, true)
	c.Assert(dbUser.isValidPwd("password1"), Equals, false)
}
