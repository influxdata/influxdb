package meta_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

// Ensure the data can be deeply copied.
func TestData_Clone(t *testing.T) {
	data := meta.Data{
		Version: 10,
		Nodes: []meta.NodeInfo{
			{ID: 1, Host: "host0"},
			{ID: 2, Host: "host1"},
		},
		Databases: []meta.DatabaseInfo{
			{
				Name: "db0",
				DefaultRetentionPolicy: "default",
				Policies: []meta.RetentionPolicyInfo{
					{
						Name:               "rp0",
						ReplicaN:           3,
						Duration:           10 * time.Second,
						ShardGroupDuration: 3 * time.Millisecond,
						ShardGroups: []meta.ShardGroupInfo{
							{
								ID:        100,
								StartTime: time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
								EndTime:   time.Date(2000, time.February, 1, 0, 0, 0, 0, time.UTC),
								Shards: []meta.ShardInfo{
									{
										ID:       200,
										OwnerIDs: []uint64{1, 3, 4},
									},
								},
							},
						},
					},
				},
				ContinuousQueries: []meta.ContinuousQueryInfo{},
			},
		},
		Users: []meta.UserInfo{
			{
				Name:       "susy",
				Hash:       "ABC123",
				Admin:      true,
				Privileges: map[string]influxql.Privilege{"db0": influxql.AllPrivileges},
			},
		},
	}

	// Copy the root structure.
	other := data.Clone()

	// Version should be incremented.
	if other.Version != 11 {
		t.Fatal("version not incremented")
	} else if !reflect.DeepEqual(data.Nodes, other.Nodes) {
		t.Fatalf("unexpected cloned nodes: %#v", other.Nodes)
	} else if !reflect.DeepEqual(data.Databases, other.Databases) {
		t.Fatalf("unexpected cloned databases: %#v", other.Databases)
	} else if !reflect.DeepEqual(data.Users, other.Users) {
		t.Fatalf("unexpected cloned users: %#v", other.Users)
	}

	// Ensure that changing data in the clone does not affect the original.
	other.Databases[0].Policies[0].ShardGroups[0].Shards[0].OwnerIDs[1] = 9
	if v := data.Databases[0].Policies[0].ShardGroups[0].Shards[0].OwnerIDs[1]; v != 3 {
		t.Fatalf("editing clone changed original: %v", v)
	}
}

// Ensure the node info can be encoded to and from a binary format.
func TestNodeInfo_Marshal(t *testing.T) {
	// Encode object.
	n := meta.NodeInfo{ID: 100, Host: "server0"}
	buf, err := n.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// Decode object.
	var other meta.NodeInfo
	if err := other.UnmarshalBinary(buf); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(n, other) {
		t.Fatalf("mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", n, other)
	}
}
