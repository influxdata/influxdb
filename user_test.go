package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

// Ensure the user's password can be changed.
func TestUser_ChangePassword(t *testing.T) {
	u := &influxdb.ClusterAdmin{influxdb.CommonUser{"root", "", false, "root"}}
	if err := u.ChangePassword("password"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

/*
func (self *UserSuite) TestProperties(c *C) {
	u := ClusterAdmin{CommonUser{Name: "root"}}
	c.Assert(u.IsClusterAdmin(), Equals, true)
	c.Assert(u.IsDbAdmin("db"), Equals, true)
	c.Assert(u.GetName(), Equals, "root")
	hash, err := HashPassword("foobar")
	c.Assert(err, IsNil)
	c.Assert(u.ChangePassword(string(hash)), IsNil)
	c.Assert(u.isValidPwd("foobar"), Equals, true)
	c.Assert(u.isValidPwd("password"), Equals, false)

	dbUser := DbUser{CommonUser{Name: "db_user"}, "db", nil, nil, true}
	c.Assert(dbUser.IsClusterAdmin(), Equals, false)
	c.Assert(dbUser.IsDbAdmin("db"), Equals, true)
	c.Assert(dbUser.GetName(), Equals, "db_user")
	hash, err = HashPassword("password")
	c.Assert(err, IsNil)
	c.Assert(dbUser.ChangePassword(string(hash)), IsNil)
	c.Assert(dbUser.isValidPwd("password"), Equals, true)
	c.Assert(dbUser.isValidPwd("password1"), Equals, false)
}
*/
