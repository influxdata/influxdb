package meta_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxql"

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

func TestData_TruncateShardGroups(t *testing.T) {
	data := &meta.Data{}

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	must(data.CreateDatabase("db"))
	rp := meta.NewRetentionPolicyInfo("rp")
	rp.ShardGroupDuration = 24 * time.Hour
	must(data.CreateRetentionPolicy("db", rp, true))

	must(data.CreateShardGroup("db", "rp", time.Unix(0, 0)))

	sg0, err := data.ShardGroupByTimestamp("db", "rp", time.Unix(0, 0))
	if err != nil {
		t.Fatal("Failed to find shard group:", err)
	}

	if sg0.Truncated() {
		t.Fatal("shard group already truncated")
	}

	sgEnd, err := data.ShardGroupByTimestamp("db", "rp", sg0.StartTime.Add(rp.ShardGroupDuration-1))
	if err != nil {
		t.Fatal("Failed to find shard group for end range:", err)
	}

	if sgEnd == nil || sgEnd.ID != sg0.ID {
		t.Fatalf("Retention policy mis-match: Expected %v, Got %v", sg0, sgEnd)
	}

	must(data.CreateShardGroup("db", "rp", sg0.StartTime.Add(rp.ShardGroupDuration)))

	sg1, err := data.ShardGroupByTimestamp("db", "rp", sg0.StartTime.Add(rp.ShardGroupDuration+time.Minute))
	if err != nil {
		t.Fatal("Failed to find second shard group:", err)
	}

	if sg1.Truncated() {
		t.Fatal("second shard group already truncated")
	}

	// shouldn't do anything
	must(data.CreateShardGroup("db", "rp", sg0.EndTime.Add(-time.Minute)))

	sgs, err := data.ShardGroupsByTimeRange("db", "rp", time.Unix(0, 0), sg1.EndTime.Add(time.Minute))
	if err != nil {
		t.Fatal("Failed to find shard groups:", err)
	}

	if len(sgs) != 2 {
		t.Fatalf("Expected %d shard groups, found %d", 2, len(sgs))
	}

	truncateTime := sg0.EndTime.Add(-time.Minute)
	data.TruncateShardGroups(truncateTime)

	// at this point, we should get nil shard groups for times after truncateTime
	for _, tc := range []struct {
		t      time.Time
		exists bool
	}{
		{sg0.StartTime, true},
		{sg0.EndTime.Add(-1), false},
		{truncateTime.Add(-1), true},
		{truncateTime, false},
		{sg1.StartTime, false},
	} {
		sg, err := data.ShardGroupByTimestamp("db", "rp", tc.t)
		if err != nil {
			t.Fatalf("Failed to find shardgroup for %v: %v", tc.t, err)
		}
		if tc.exists && sg == nil {
			t.Fatalf("Shard group for timestamp '%v' should exist, got nil", tc.t)
		}
	}

	for _, x := range data.Databases[0].RetentionPolicies[0].ShardGroups {
		switch x.ID {
		case sg0.ID:
			*sg0 = x
		case sg1.ID:
			*sg1 = x
		}
	}

	if sg0.TruncatedAt != truncateTime {
		t.Fatalf("Incorrect truncation of current shard group. Expected %v, got %v", truncateTime, sg0.TruncatedAt)
	}

	if sg1.TruncatedAt != sg1.StartTime {
		t.Fatalf("Incorrect truncation of future shard group. Expected %v, got %v", sg1.StartTime, sg1.TruncatedAt)
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

func TestShardGroupInfo_Contains(t *testing.T) {
	sgi := &meta.ShardGroupInfo{StartTime: time.Unix(10, 0), EndTime: time.Unix(20, 0)}

	tests := []struct {
		ts  time.Time
		exp bool
	}{
		{time.Unix(0, 0), false},
		{time.Unix(9, 0), false},
		{time.Unix(10, 0), true},
		{time.Unix(11, 0), true},
		{time.Unix(15, 0), true},
		{time.Unix(19, 0), true},
		{time.Unix(20, 0), false},
		{time.Unix(21, 0), false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("ts=%d", test.ts.Unix()), func(t *testing.T) {
			got := sgi.Contains(test.ts)
			assert.Equal(t, got, test.exp)
		})
	}
}
