package meta_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxql"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

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

func Test_Data_CreateDatabase(t *testing.T) {
	data := meta.Data{}

	// Test creating a database succeedes.
	if err := data.CreateDatabase("foo"); err != nil {
		t.Fatal(err)
	}

	// Test creating a database with a name that is too long fails.
	name := randString(meta.MaxNameLen + 1)
	if err := data.CreateDatabase(name); err != meta.ErrNameTooLong {
		t.Fatalf("exp: %v, got: %v", meta.ErrNameTooLong, err)
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

	// Try creating a retention policy with a name that is too long. Should fail.
	err = data.CreateRetentionPolicy("foo", &meta.RetentionPolicyInfo{
		Name:     randString(meta.MaxNameLen + 1),
		ReplicaN: 1,
		Duration: 24 * time.Hour,
	}, true)
	if err != meta.ErrNameTooLong {
		t.Fatalf("exp: %v, got %v", meta.ErrNameTooLong, err)
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

	groups := data.Databases[0].RetentionPolicies[0].ShardGroups
	assert.Equal(t, 2, len(groups))
	assert.Equal(t, "1970-01-01 00:00:00 +0000 UTC", groups[0].StartTime.String())
	assert.Equal(t, "1970-01-02 00:00:00 +0000 UTC", groups[0].EndTime.String())
	assert.Equal(t, "1970-01-01 23:59:00 +0000 UTC", groups[0].TruncatedAt.String())

	assert.Equal(t, "1970-01-02 00:00:00 +0000 UTC", groups[1].StartTime.String())
	assert.Equal(t, "1970-01-03 00:00:00 +0000 UTC", groups[1].EndTime.String())
	assert.Equal(t, "1970-01-02 00:00:00 +0000 UTC", groups[1].TruncatedAt.String())

	// Create some more shard groups and validate there is no overlap
	// Add a shard starting at sg0's truncation time, until 01/02
	must(data.CreateShardGroup("db", "rp", sg0.EndTime.Add(-time.Second)))
	// Add a shard 01/02 - 01/03 (since sg1 is fully truncated)
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(-time.Second)))
	// Add a shard 01/06 - 01/07
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(3*rp.ShardGroupDuration)))
	newDuration := 10 * rp.ShardGroupDuration
	data.UpdateRetentionPolicy("db", "rp", &meta.RetentionPolicyUpdate{
		Name:               nil,
		Duration:           nil,
		ReplicaN:           nil,
		ShardGroupDuration: &newDuration,
	}, true)
	// Add a shard 01/03 - 01/06
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(1*rp.ShardGroupDuration)))
	// Add a shard 01/07 - 01/09
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(4*rp.ShardGroupDuration)))
	// Add a shard 01/09 - 01/19
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(10*rp.ShardGroupDuration)))
	// No additional shard added
	must(data.CreateShardGroup("db", "rp", sg1.EndTime.Add(11*rp.ShardGroupDuration)))

	groups = data.Databases[0].RetentionPolicies[0].ShardGroups
	assert.Equal(t, 8, len(groups))

	expectTimes := []struct {
		start, end, truncated string
	}{
		{"1970-01-01 00:00:00 +0000 UTC", "1970-01-02 00:00:00 +0000 UTC", "1970-01-01 23:59:00 +0000 UTC"},
		{"1970-01-01 23:59:00 +0000 UTC", "1970-01-02 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
		{"1970-01-02 00:00:00 +0000 UTC", "1970-01-03 00:00:00 +0000 UTC", "1970-01-02 00:00:00 +0000 UTC"},
		{"1970-01-02 00:00:00 +0000 UTC", "1970-01-03 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
		{"1970-01-03 00:00:00 +0000 UTC", "1970-01-06 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
		{"1970-01-06 00:00:00 +0000 UTC", "1970-01-07 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
		{"1970-01-07 00:00:00 +0000 UTC", "1970-01-09 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
		{"1970-01-09 00:00:00 +0000 UTC", "1970-01-19 00:00:00 +0000 UTC", "0001-01-01 00:00:00 +0000 UTC"},
	}

	for i := range expectTimes {
		assert.Equal(t, expectTimes[i].start, groups[i].StartTime.String(), "start time %d", i)
		assert.Equal(t, expectTimes[i].end, groups[i].EndTime.String(), "end time %d", i)
		assert.Equal(t, expectTimes[i].truncated, groups[i].TruncatedAt.String(), "truncate time %d", i)
	}

}

func TestData_TruncateShardGroups_FutureOverlappingShards(t *testing.T) {
	data := &meta.Data{}

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	must(data.CreateDatabase("db"))
	rp := meta.NewRetentionPolicyInfo("rp")
	rp.ShardGroupDuration = 72 * time.Hour // 72 hours as per the issue description
	must(data.CreateRetentionPolicy("db", rp, true))

	// Reproduce the exact scenario from the issue description:
	// Step 1: Insert a data point in the future (this creates a shard)
	futureTime := time.Unix(0, 0).Add(365 * 24 * time.Hour) // 1 year in future
	must(data.CreateShardGroup("db", "rp", futureTime))
	
	initialShardCount := len(data.Databases[0].RetentionPolicies[0].ShardGroups)
	t.Logf("After initial shard creation: %d shards", initialShardCount)
	
	// Step 2: Run truncate-shards
	truncateTime := time.Now().UTC()
	data.TruncateShardGroups(truncateTime)
	
	afterTruncateCount := len(data.Databases[0].RetentionPolicies[0].ShardGroups)
	t.Logf("After truncate-shards: %d shards", afterTruncateCount)
	
	// Step 3: Insert the same datapoint again into the future
	// This should NOT create a new overlapping shard
	must(data.CreateShardGroup("db", "rp", futureTime))
	
	finalShardCount := len(data.Databases[0].RetentionPolicies[0].ShardGroups)
	t.Logf("After re-inserting same future timestamp: %d shards", finalShardCount)
	
	// The bug would manifest as creating a second shard for the same time range
	if finalShardCount > afterTruncateCount {
		t.Errorf("BUG REPRODUCED: Additional shard was created for same future timestamp after truncation")
	}
	
	// Step 4: Verify no overlapping shards exist
	groups := data.Databases[0].RetentionPolicies[0].ShardGroups
	
	// Debug: Print all shard groups
	t.Logf("Total shard groups after operations: %d", len(groups))
	for i, group := range groups {
		effectiveEnd := group.EndTime
		if group.Truncated() {
			effectiveEnd = group.TruncatedAt
		}
		t.Logf("Shard %d: [%v - %v) effective [%v - %v) truncated=%v deleted=%v", 
			i, group.StartTime, group.EndTime, group.StartTime, effectiveEnd, 
			group.Truncated(), group.Deleted())
	}
	
	// Check that we don't have overlapping time coverage for the same time range
	for i := 0; i < len(groups); i++ {
		for j := i + 1; j < len(groups); j++ {
			if groups[i].Deleted() || groups[j].Deleted() {
				continue
			}
			
			// Calculate effective ranges considering truncation
			startI, endI := groups[i].StartTime, groups[i].EndTime
			if groups[i].Truncated() {
				endI = groups[i].TruncatedAt
			}
			
			startJ, endJ := groups[j].StartTime, groups[j].EndTime
			if groups[j].Truncated() {
				endJ = groups[j].TruncatedAt
			}
			
			// Check for overlapping coverage - ranges [a,b) and [c,d) overlap if a < d && c < b
			if startI.Before(endJ) && startJ.Before(endI) {
				t.Fatalf("Found overlapping shard groups after truncate-shards:\n"+
					"  Shard %d: [%v - %v) effective [%v - %v)\n"+
					"  Shard %d: [%v - %v) effective [%v - %v)",
					i, groups[i].StartTime, groups[i].EndTime, startI, endI,
					j, groups[j].StartTime, groups[j].EndTime, startJ, endJ)
			}
		}
	}
	
	// Additional check: ensure the future timestamp has exactly one covering shard
	coveringShards := 0
	for _, group := range groups {
		if group.Deleted() {
			continue
		}
		
		effectiveEnd := group.EndTime
		if group.Truncated() {
			effectiveEnd = group.TruncatedAt
		}
		
		// Check if this shard covers our future time
		if !futureTime.Before(group.StartTime) && futureTime.Before(effectiveEnd) {
			coveringShards++
		}
	}
	
	if coveringShards != 1 {
		t.Fatalf("Expected exactly 1 shard to cover future time %v, but found %d covering shards", 
			futureTime, coveringShards)
	}
}

// TestData_TruncateShardGroups_ActualOverlapBug reproduces the exact issue described:
// 1. Create database with 72h shard duration  
// 2. Insert data point in future
// 3. Run truncate-shards with time that's after the start but before end of future shard
// 4. Insert same datapoint again - this should create overlapping shards
func TestData_TruncateShardGroups_ActualOverlapBug(t *testing.T) {
	data := &meta.Data{}

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	// Setup database with 72 hour shard group duration
	must(data.CreateDatabase("db"))
	rp := meta.NewRetentionPolicyInfo("rp")
	rp.ShardGroupDuration = 72 * time.Hour
	must(data.CreateRetentionPolicy("db", rp, true))

	// Step 1: Insert a data point in the future (creates a shard)
	futureTime := time.Unix(0, 0).Add(365 * 24 * time.Hour) // 1 year in future
	must(data.CreateShardGroup("db", "rp", futureTime))
	
	// Get the created shard group to understand its time range
	sg, err := data.ShardGroupByTimestamp("db", "rp", futureTime)
	if err != nil || sg == nil {
		t.Fatal("Failed to create or find future shard group")
	}
	
	t.Logf("Created future shard: [%v - %v)", sg.StartTime, sg.EndTime)
	
	// Step 2: Run truncate-shards with a time that's WITHIN the future shard's range
	// This should truncate the future shard
	truncateTime := sg.StartTime.Add(24 * time.Hour) // 24 hours into the 72-hour shard
	t.Logf("Truncating at: %v", truncateTime)
	data.TruncateShardGroups(truncateTime)
	
	// Refresh the shard group reference after truncation
	groups := data.Databases[0].RetentionPolicies[0].ShardGroups
	for i := range groups {
		if groups[i].ID == sg.ID {
			*sg = groups[i]
			break
		}
	}
	
	t.Logf("After truncation - Shard: [%v - %v), truncated=%v, truncatedAt=%v", 
		sg.StartTime, sg.EndTime, sg.Truncated(), sg.TruncatedAt)
	
	// Step 3: Try to insert data at a time that falls in the truncated portion  
	// This is the critical test - timestamp after truncation but within original shard range
	timeInTruncatedRange := sg.StartTime.Add(48 * time.Hour) // Beyond truncation but within original range
	beforeCount := len(data.Databases[0].RetentionPolicies[0].ShardGroups)
	must(data.CreateShardGroup("db", "rp", timeInTruncatedRange))
	afterCount := len(data.Databases[0].RetentionPolicies[0].ShardGroups)
	
	t.Logf("Shard count before: %d, after creating shard for truncated range: %d", beforeCount, afterCount)
	
	// Print all shards for debugging
	groups = data.Databases[0].RetentionPolicies[0].ShardGroups
	for i, group := range groups {
		effectiveEnd := group.EndTime
		if group.Truncated() {
			effectiveEnd = group.TruncatedAt
		}
		t.Logf("Shard %d: [%v - %v) effective [%v - %v) truncated=%v", 
			i, group.StartTime, group.EndTime, group.StartTime, effectiveEnd, group.Truncated())
	}
	
	// Check for the bug: ensure timestamp coverage is not duplicated
	testTimestamp := timeInTruncatedRange
	coveringShards := 0
	var coveringShardDetails []string
	
	for i, group := range groups {
		if group.Deleted() {
			continue
		}
		
		effectiveEnd := group.EndTime
		if group.Truncated() {
			effectiveEnd = group.TruncatedAt
		}
		
		// Check if this shard covers our test timestamp
		if !testTimestamp.Before(group.StartTime) && testTimestamp.Before(effectiveEnd) {
			coveringShards++
			coveringShardDetails = append(coveringShardDetails, 
				fmt.Sprintf("Shard %d [%v - %v)", i, group.StartTime, effectiveEnd))
		}
	}
	
	t.Logf("Timestamp %v coverage:", testTimestamp)
	for _, detail := range coveringShardDetails {
		t.Logf("  %s", detail)
	}
	
	if coveringShards > 1 {
		t.Errorf("BUG REPRODUCED: Timestamp %v is covered by %d shards (should be 1)", 
			testTimestamp, coveringShards)
	} else if coveringShards == 0 {
		t.Errorf("COVERAGE GAP: Timestamp %v is not covered by any shard", testTimestamp)
	} else {
		t.Logf("PASS: Timestamp %v is covered by exactly 1 shard", testTimestamp)
	}
}

// TestData_ShardGroupsByTimeRange_TruncatedShardsBug demonstrates the bug where
// truncated shards are incorrectly included in time range queries
func TestData_ShardGroupsByTimeRange_TruncatedShardsBug(t *testing.T) {
	data := &meta.Data{}

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	must(data.CreateDatabase("db"))
	rp := meta.NewRetentionPolicyInfo("rp")
	rp.ShardGroupDuration = 72 * time.Hour
	must(data.CreateRetentionPolicy("db", rp, true))

	// Create future shard
	futureTime := time.Unix(0, 0).Add(365 * 24 * time.Hour)
	must(data.CreateShardGroup("db", "rp", futureTime))
	
	sg, err := data.ShardGroupByTimestamp("db", "rp", futureTime)
	if err != nil || sg == nil {
		t.Fatal("Failed to create future shard group")
	}
	
	t.Logf("Created shard: [%v - %v)", sg.StartTime, sg.EndTime)
	
	// Truncate the shard partway through
	truncateTime := sg.StartTime.Add(24 * time.Hour)
	data.TruncateShardGroups(truncateTime)
	
	// Create a new shard for the truncated range
	timeInTruncatedRange := sg.StartTime.Add(48 * time.Hour)
	must(data.CreateShardGroup("db", "rp", timeInTruncatedRange))
	
	// BUG: Query for time range that should only hit the new shard
	// but will incorrectly include the truncated shard due to Overlaps() bug
	queryStart := truncateTime.Add(12 * time.Hour) // After truncation
	queryEnd := queryStart.Add(6 * time.Hour)      // Well after truncation
	
	shards, err := data.ShardGroupsByTimeRange("db", "rp", queryStart, queryEnd)
	if err != nil {
		t.Fatal("Failed to get shards by time range:", err)
	}
	
	t.Logf("Query range: [%v - %v)", queryStart, queryEnd)
	t.Logf("ShardGroupsByTimeRange returned %d shards:", len(shards))
	
	for i, shard := range shards {
		effectiveEnd := shard.EndTime
		if shard.Truncated() {
			effectiveEnd = shard.TruncatedAt
		}
		t.Logf("  Shard %d: [%v - %v) effective [%v - %v) truncated=%v", 
			i, shard.StartTime, shard.EndTime, shard.StartTime, effectiveEnd, shard.Truncated())
		
		// Check if this shard should actually be included
		shouldBeIncluded := !queryStart.Before(shard.StartTime) && !queryEnd.Before(shard.StartTime) ||
			                 !shard.StartTime.After(queryEnd) && effectiveEnd.After(queryStart)
		
		actuallyIncluded := !shard.StartTime.After(queryEnd) && shard.EndTime.After(queryStart)
		
		if actuallyIncluded && !shouldBeIncluded {
			t.Errorf("BUG DETECTED: Truncated shard incorrectly included in query results")
			t.Errorf("  Shard effective range: [%v - %v)", shard.StartTime, effectiveEnd)
			t.Errorf("  Query range: [%v - %v)", queryStart, queryEnd)
			t.Errorf("  Shard should NOT be included because effective end %v <= query start %v", 
				effectiveEnd, queryStart)
		}
	}
	
	// Verify the fix: only the correct shard should be returned
	if len(shards) != 1 {
		t.Errorf("Expected exactly 1 shard for query range, got %d", len(shards))
		if len(shards) > 1 {
			t.Errorf("Multiple shards would cause duplicate data points in query results")
		}
	} else {
		// Verify it's the correct shard (the non-truncated one)
		shard := shards[0]
		if shard.Truncated() {
			t.Errorf("Query returned truncated shard, should return the non-truncated shard")
		} else {
			t.Logf("SUCCESS: Query correctly returned only the non-truncated shard")
		}
	}
}

// TestShardGroupInfo_Contains_TruncatedShards verifies Contains method respects truncation
func TestShardGroupInfo_Contains_TruncatedShards(t *testing.T) {
	data := &meta.Data{}

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	must(data.CreateDatabase("db"))
	rp := meta.NewRetentionPolicyInfo("rp")
	rp.ShardGroupDuration = 72 * time.Hour
	must(data.CreateRetentionPolicy("db", rp, true))

	// Create and truncate a shard group
	futureTime := time.Unix(0, 0).Add(365 * 24 * time.Hour)
	must(data.CreateShardGroup("db", "rp", futureTime))
	
	sg, err := data.ShardGroupByTimestamp("db", "rp", futureTime)
	if err != nil || sg == nil {
		t.Fatal("Failed to create future shard group")
	}
	
	// Truncate the shard partway through
	truncateTime := sg.StartTime.Add(24 * time.Hour)
	data.TruncateShardGroups(truncateTime)
	
	// Refresh the shard group reference
	groups := data.Databases[0].RetentionPolicies[0].ShardGroups
	for i := range groups {
		if groups[i].ID == sg.ID {
			*sg = groups[i]
			break
		}
	}
	
	// Test timestamps
	beforeStart := sg.StartTime.Add(-1 * time.Hour)
	atStart := sg.StartTime
	beforeTruncation := sg.StartTime.Add(12 * time.Hour)
	atTruncation := sg.TruncatedAt
	afterTruncation := sg.StartTime.Add(36 * time.Hour)
	beforeOriginalEnd := sg.EndTime.Add(-1 * time.Hour)
	atOriginalEnd := sg.EndTime
	
	testCases := []struct {
		name      string
		timestamp time.Time
		expected  bool
	}{
		{"before start", beforeStart, false},
		{"at start", atStart, true},
		{"before truncation", beforeTruncation, true},
		{"at truncation", atTruncation, false}, // Truncation point is exclusive
		{"after truncation", afterTruncation, false},
		{"before original end", beforeOriginalEnd, false},
		{"at original end", atOriginalEnd, false},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := sg.Contains(tc.timestamp)
			if result != tc.expected {
				t.Errorf("Contains(%v) = %v, expected %v", tc.timestamp, result, tc.expected)
				t.Errorf("Shard: [%v - %v), truncated at %v", sg.StartTime, sg.EndTime, sg.TruncatedAt)
			}
		})
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

func randString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
