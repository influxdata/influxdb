package coordinator_test

import (
	"context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"reflect"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/dbrp/mocks"
	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/internal"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/v1/coordinator"
	"github.com/influxdata/influxdb/v2/v1/services/meta"
	"github.com/influxdata/influxql"
)

func TestLocalShardMapper(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbrp := mocks.NewMockDBRPMappingService(ctrl)
	orgID := platform.ID(0xff00)
	bucketID := platform.ID(0xffee)
	db := "db0"
	rp := "rp0"
	filt := influxdb.DBRPMappingFilter{OrgID: &orgID, Database: &db, RetentionPolicy: &rp}
	res := []*influxdb.DBRPMapping{{Database: db, RetentionPolicy: rp, OrganizationID: orgID, BucketID: bucketID}}
	dbrp.EXPECT().
		FindMany(gomock.Any(), filt).
		Times(2).
		Return(res, 1, nil)

	var metaClient MetaClient
	metaClient.ShardGroupsByTimeRangeFn = func(database, policy string, min, max time.Time) ([]meta.ShardGroupInfo, error) {
		if database != bucketID.String() {
			t.Errorf("unexpected database: %s", database)
		}
		if policy != meta.DefaultRetentionPolicyName {
			t.Errorf("unexpected retention policy: %s", policy)
		}
		return []meta.ShardGroupInfo{
			{ID: 1, Shards: []meta.ShardInfo{
				{ID: 1, Owners: []meta.ShardOwner{{NodeID: 0}}},
				{ID: 2, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
			{ID: 2, Shards: []meta.ShardInfo{
				{ID: 3, Owners: []meta.ShardOwner{{NodeID: 0}}},
				{ID: 4, Owners: []meta.ShardOwner{{NodeID: 0}}},
			}},
		}, nil
	}

	tsdbStore := &internal.TSDBStoreMock{}
	tsdbStore.ShardGroupFn = func(ids []uint64) tsdb.ShardGroup {
		if !reflect.DeepEqual(ids, []uint64{1, 2, 3, 4}) {
			t.Errorf("unexpected shard ids: %#v", ids)
		}

		var sh MockShard
		sh.CreateIteratorFn = func(ctx context.Context, measurement *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
			if measurement.Name != "cpu" {
				t.Errorf("unexpected measurement: %s", measurement.Name)
			}
			return &FloatIterator{}, nil
		}
		return &sh
	}

	// Initialize the shard mapper.
	shardMapper := &coordinator.LocalShardMapper{
		MetaClient: &metaClient,
		TSDBStore:  tsdbStore,
		DBRP:       dbrp,
	}

	// Normal measurement.
	measurement := &influxql.Measurement{
		Database:        db,
		RetentionPolicy: rp,
		Name:            "cpu",
	}
	ic, err := shardMapper.MapShards(context.Background(), []influxql.Source{measurement}, influxql.TimeRange{}, query.SelectOptions{OrgID: orgID})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// This should be a LocalShardMapping.
	m, ok := ic.(*coordinator.LocalShardMapping)
	if !ok {
		t.Fatalf("unexpected mapping type: %T", ic)
	} else if len(m.ShardMap) != 1 {
		t.Fatalf("unexpected number of shard mappings: %d", len(m.ShardMap))
	}

	if _, err := ic.CreateIterator(context.Background(), measurement, query.IteratorOptions{OrgID: orgID}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Subquery.
	subquery := &influxql.SubQuery{
		Statement: &influxql.SelectStatement{
			Sources: []influxql.Source{measurement},
		},
	}
	ic, err = shardMapper.MapShards(context.Background(), []influxql.Source{subquery}, influxql.TimeRange{}, query.SelectOptions{OrgID: orgID})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// This should be a LocalShardMapping.
	m, ok = ic.(*coordinator.LocalShardMapping)
	if !ok {
		t.Fatalf("unexpected mapping type: %T", ic)
	} else if len(m.ShardMap) != 1 {
		t.Fatalf("unexpected number of shard mappings: %d", len(m.ShardMap))
	}

	if _, err := ic.CreateIterator(context.Background(), measurement, query.IteratorOptions{OrgID: orgID}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}
