package influxdb_test

import (
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID   platform.ID
		ResourceType influxdb.ResourceType
		UserID       platform.ID
		UserType     influxdb.UserType
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid mapping",
			fields: fields{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Owner,
				ResourceType: influxdb.DashboardsResourceType,
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
			},
		},
		{
			name: "mapping requires a resourceid",
			fields: fields{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Owner,
				ResourceType: influxdb.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a userid",
			fields: fields{
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
				UserType:     influxdb.Owner,
				ResourceType: influxdb.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a usertype",
			fields: fields{
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				ResourceType: influxdb.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a resourcetype",
			fields: fields{
				ResourceID: influxdbtesting.MustIDBase16("020f755c3c082000"),
				UserID:     influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:   influxdb.Owner,
			},
			wantErr: true,
		},
		{
			name: "the usertype provided must be valid",
			fields: fields{
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     "foo",
				ResourceType: influxdb.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "the resourcetype provided must be valid",
			fields: fields{
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Owner,
				ResourceType: "foo",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := influxdb.UserResourceMapping{
				ResourceID:   tt.fields.ResourceID,
				ResourceType: tt.fields.ResourceType,
				UserID:       tt.fields.UserID,
				UserType:     tt.fields.UserType,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOwnerMappingToPermissions(t *testing.T) {
	type wants struct {
		perms influxdb.Permission
		err   bool
	}

	ResourceID, _ := platform.IDFromString("020f755c3c082000")

	tests := []struct {
		name  string
		urm   influxdb.UserResourceMapping
		wants wants
	}{
		{
			name: "Org Member Has Permission To Read Org",
			urm: influxdb.UserResourceMapping{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Member,
				ResourceType: influxdb.OrgsResourceType,
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Org Owner Has Permission To Write Org",
			urm: influxdb.UserResourceMapping{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Owner,
				ResourceType: influxdb.OrgsResourceType,
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "write", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Org Owner Has Permission To Read Org",
			urm: influxdb.UserResourceMapping{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Owner,
				ResourceType: influxdb.OrgsResourceType,
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Bucket Member User Has Permission To Read Bucket",
			urm: influxdb.UserResourceMapping{
				UserID:       influxdbtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     influxdb.Member,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   influxdbtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "buckets", ID: ResourceID}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perms, err := tt.urm.ToPermissions()

			require.Contains(t, perms, tt.wants.perms)
			require.Equal(t, tt.wants.err, err != nil)
		})
	}
}
