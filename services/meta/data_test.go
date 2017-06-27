package meta_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/influxql"

	"github.com/influxdata/influxdb/services/meta"
)

func Test_Data_DropDatabase(t *testing.T) {
	data := &meta.Data{
		Databases: []meta.DatabaseInfo{
			{Name: "db0"},
			{Name: "db1"},
			{Name: "db2"},
			{Name: "db4"},
			{Name: "db5"},
		},
		Users: []meta.UserInfo{
			{Name: "user1", Privileges: map[string]influxql.Privilege{"db1": influxql.ReadPrivilege, "db2": influxql.ReadPrivilege}},
			{Name: "user2", Privileges: map[string]influxql.Privilege{"db2": influxql.ReadPrivilege}},
		},
	}

	// Dropping the first database removes it from the Data object.
	expDbs := make([]meta.DatabaseInfo, 4)
	copy(expDbs, data.Databases[1:])
	if err := data.DropDatabase("db0"); err != nil {
		t.Fatal(err)
	} else if got, exp := data.Databases, expDbs; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Dropping a middle database removes it from the data object.
	expDbs = []meta.DatabaseInfo{{Name: "db1"}, {Name: "db2"}, {Name: "db5"}}
	if err := data.DropDatabase("db4"); err != nil {
		t.Fatal(err)
	} else if got, exp := data.Databases, expDbs; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Dropping the last database removes it from the data object.
	expDbs = []meta.DatabaseInfo{{Name: "db1"}, {Name: "db2"}}
	if err := data.DropDatabase("db5"); err != nil {
		t.Fatal(err)
	} else if got, exp := data.Databases, expDbs; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Dropping a database also drops all the user privileges associated with
	// it.
	expUsers := []meta.UserInfo{
		{Name: "user1", Privileges: map[string]influxql.Privilege{"db1": influxql.ReadPrivilege}},
		{Name: "user2", Privileges: map[string]influxql.Privilege{}},
	}
	if err := data.DropDatabase("db2"); err != nil {
		t.Fatal(err)
	} else if got, exp := data.Users, expUsers; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, exp)
	}
}

func Test_Data_CreateRetentionPolicy(t *testing.T) {
	data := meta.Data{}

	err := data.CreateDatabase("foo")
	if err != nil {
		t.Fatal(err)
	}

	err = data.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{
		Name:     "bar",
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, false)
	if err != nil {
		t.Fatal(err)
	}

	rp, err := data.RetentionPolicy("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	if rp == nil {
		t.Fatal("creation of retention policy failed")
	}

	// Try to recreate the same RP with default set to true, should fail
	err = data.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{
		Name:     "bar",
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, true)
	if err == nil || err != meta.ErrRetentionPolicyConflict {
		t.Fatalf("unexpected error.  got: %v, exp: %s", err, meta.ErrRetentionPolicyConflict)
	}

	// Creating the same RP with the same specifications should succeed
	err = data.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{
		Name:     "bar",
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
}

func TestData_AdminUserExists(t *testing.T) {
	data := meta.Data{}

	// No users means no admin.
	if data.AdminUserExists() {
		t.Fatal("no admin user should exist")
	}

	// Add a non-admin user.
	if err := data.CreateUser("user1", "a", false); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Add an admin user.
	if err := data.CreateUser("admin1", "a", true); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Remove the original user
	if err := data.DropUser("user1"); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Add another admin
	if err := data.CreateUser("admin2", "a", true); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Revoke privileges of the first admin
	if err := data.SetAdminPrivilege("admin1", false); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Add user1 back.
	if err := data.CreateUser("user1", "a", false); err != nil {
		t.Fatal(err)
	}
	// Revoke remaining admin.
	if err := data.SetAdminPrivilege("admin2", false); err != nil {
		t.Fatal(err)
	}
	// No longer any admins
	if got, exp := data.AdminUserExists(), false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Make user1 an admin
	if err := data.SetAdminPrivilege("user1", true); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Drop user1...
	if err := data.DropUser("user1"); err != nil {
		t.Fatal(err)
	}
	if got, exp := data.AdminUserExists(), false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}
}

func TestData_SetPrivilege(t *testing.T) {
	data := meta.Data{}
	if err := data.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	if err := data.CreateUser("user1", "", false); err != nil {
		t.Fatal(err)
	}

	// When the user does not exist, SetPrivilege returns an error.
	if got, exp := data.SetPrivilege("not a user", "db0", influxql.AllPrivileges), meta.ErrUserNotFound; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// When the database does not exist, SetPrivilege returns an error.
	if got, exp := data.SetPrivilege("user1", "db1", influxql.AllPrivileges), influxdb.ErrDatabaseNotFound("db1"); got == nil || got.Error() != exp.Error() {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// Otherwise, SetPrivilege sets the expected privileges.
	if got := data.SetPrivilege("user1", "db0", influxql.AllPrivileges); got != nil {
		t.Fatalf("got %v, expected %v", got, nil)
	}
}

func TestUserInfo_AuthorizeDatabase(t *testing.T) {
	emptyUser := &meta.UserInfo{}
	if !emptyUser.AuthorizeDatabase(influxql.NoPrivileges, "anydb") {
		t.Fatal("expected NoPrivileges to be authorized but it wasn't")
	}
	if emptyUser.AuthorizeDatabase(influxql.ReadPrivilege, "anydb") {
		t.Fatal("expected ReadPrivilege to prevent authorization, but it was authorized")
	}

	adminUser := &meta.UserInfo{Admin: true}
	if !adminUser.AuthorizeDatabase(influxql.AllPrivileges, "anydb") {
		t.Fatalf("expected admin to be authorized but it wasn't")
	}
}
