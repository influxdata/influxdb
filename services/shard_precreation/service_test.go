package shard_precreation

import (
	"testing"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/toml"
)

func Test_ShardPrecreation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	// A test metastaore which returns 2 shard groups, only 1 of which requires a successor.
	ms := metaStore{
		VisitRetentionPoliciesFn: func(f func(d meta.DatabaseInfo, r meta.RetentionPolicyInfo)) {
			f(meta.DatabaseInfo{Name: "mydb"}, meta.RetentionPolicyInfo{Name: "myrp"})
		},
		ShardGroupsFn: func(database, policy string) ([]meta.ShardGroupInfo, error) {
			if database != "mydb" || policy != "myrp" {
				t.Fatalf("ShardGroups called with incorrect database or policy")
			}

			// Make two shard groups, 1 which needs a successor, the other does not.
			groups := make([]meta.ShardGroupInfo, 2)
			groups[0] = meta.ShardGroupInfo{
				ID:        1,
				StartTime: now.Add(-1 * time.Hour),
				EndTime:   now,
			}
			groups[1] = meta.ShardGroupInfo{
				ID:        2,
				StartTime: now,
				EndTime:   now.Add(time.Hour),
			}
			return groups, nil
		},
		CreateShardGroupIfNotExistFn: func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
			if database != "mydb" || policy != "myrp" {
				t.Fatalf("ShardGroups called with incorrect database or policy")
			}
			return &meta.ShardGroupInfo{ID: 3}, nil
		},
	}

	srv, err := NewService(Config{
		CheckInterval: toml.Duration(time.Minute),
		AdvancePeriod: toml.Duration(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("failed to create shard precreation service: %s", err.Error())
	}
	srv.MetaStore = ms

	n, err := srv.precreate(now)
	if err != nil {
		t.Fatalf("failed to precreate shards: %s", err.Error())
	}
	if n != 1 {
		t.Fatalf("incorrect number of shard groups created, exp 1, got %d", n)
	}

	return
}

// PointsWriter represents a mock impl of PointsWriter.
type metaStore struct {
	VisitRetentionPoliciesFn     func(f func(d meta.DatabaseInfo, r meta.RetentionPolicyInfo))
	ShardGroupsFn                func(database, policy string) ([]meta.ShardGroupInfo, error)
	CreateShardGroupIfNotExistFn func(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
}

func (m metaStore) IsLeader() bool {
	return true
}

func (m metaStore) VisitRetentionPolicies(f func(d meta.DatabaseInfo, r meta.RetentionPolicyInfo)) {
	m.VisitRetentionPoliciesFn(f)
}

func (m metaStore) ShardGroups(database, policy string) ([]meta.ShardGroupInfo, error) {
	return m.ShardGroupsFn(database, policy)
}

func (m metaStore) CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error) {
	return m.CreateShardGroupIfNotExistFn(database, policy, timestamp)
}
