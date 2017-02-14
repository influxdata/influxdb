package meta_test

import (
	"reflect"
	"testing"
	"time"

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
