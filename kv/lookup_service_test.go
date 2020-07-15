package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

var (
	existingBucketID = influxdb.ID(mock.FirstMockID + 3)
	firstMockID      = influxdb.ID(mock.FirstMockID)
	nonexistantID    = influxdb.ID(10001)
)

type StoreFn func(*testing.T) (kv.SchemaStore, func(), error)

func TestLookupService_Name_WithBolt(t *testing.T) {
	testLookupName(NewTestBoltStore, t)
}

func testLookupName(newStore StoreFn, t *testing.T) {
	type initFn func(context.Context, *kv.Service) error
	type args struct {
		resource influxdb.Resource
		init     initFn
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "error if id is invalid",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.DashboardsResourceType,
					ID:   influxdbtesting.IDPtr(influxdb.InvalidID()),
				},
			},
			wantErr: true,
		},
		{
			name: "error if resource is invalid",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.ResourceType("invalid"),
				},
			},
			wantErr: true,
		},
		{
			name: "authorization resource without a name returns empty string",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.AuthorizationsResourceType,
					ID:   &firstMockID,
				},
			},
			want: "",
		},
		{
			name: "task resource without a name returns empty string",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.TasksResourceType,
					ID:   &firstMockID,
				},
			},
			want: "",
		},
		{
			name: "bucket with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.BucketsResourceType,
					ID:   &existingBucketID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					o1 := &influxdb.Organization{
						Name: "o1",
					}
					_ = s.CreateOrganization(ctx, o1)
					t.Log(o1)
					return s.CreateBucket(ctx, &influxdb.Bucket{
						Name:  "b1",
						OrgID: o1.ID,
					})
				},
			},
			want: "b1",
		},
		{
			name: "bucket with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.BucketsResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
		{
			name: "dashboard with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.DashboardsResourceType,
					ID:   &firstMockID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					return s.CreateDashboard(ctx, &influxdb.Dashboard{
						Name:           "dashboard1",
						OrganizationID: 1,
					})
				},
			},
			want: "dashboard1",
		},
		{
			name: "dashboard with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.DashboardsResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
		{
			name: "org with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.OrgsResourceType,
					ID:   &firstMockID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					return s.CreateOrganization(ctx, &influxdb.Organization{
						Name: "org1",
					})
				},
			},
			want: "org1",
		},
		{
			name: "org with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.OrgsResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
		{
			name: "source with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.SourcesResourceType,
					ID:   &firstMockID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					return s.CreateSource(ctx, &influxdb.Source{
						Name: "source1",
					})
				},
			},
			want: "source1",
		},
		{
			name: "source with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.SourcesResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
		{
			name: "telegraf with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.TelegrafsResourceType,
					ID:   &firstMockID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					return s.CreateTelegrafConfig(ctx, &influxdb.TelegrafConfig{
						OrgID:  influxdbtesting.MustIDBase16("0000000000000009"),
						Name:   "telegraf1",
						Config: "[agent]",
					}, existingBucketID)
				},
			},
			want: "telegraf1",
		},
		{
			name: "telegraf with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.TelegrafsResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
		{
			name: "user with existing id returns name",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.UsersResourceType,
					ID:   &firstMockID,
				},
				init: func(ctx context.Context, s *kv.Service) error {
					return s.CreateUser(ctx, &influxdb.User{
						Name: "user1",
					})
				},
			},
			want: "user1",
		},
		{
			name: "user with non-existent id returns error",
			args: args{
				resource: influxdb.Resource{
					Type: influxdb.UsersResourceType,
					ID:   &nonexistantID,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, done, err := newStore(t)
			if err != nil {
				t.Fatalf("unable to create bolt test client: %v", err)
			}
			svc := kv.NewService(zaptest.NewLogger(t), store)
			defer done()

			svc.IDGenerator = mock.NewMockIDGenerator()
			svc.OrgBucketIDs = mock.NewMockIDGenerator()
			svc.WithSpecialOrgBucketIDs(svc.IDGenerator)
			ctx := context.Background()
			if tt.args.init != nil {
				if err := tt.args.init(ctx, svc); err != nil {
					t.Errorf("Service.Name() unable to initialize service: %v", err)
				}
			}
			id := influxdb.InvalidID()
			if tt.args.resource.ID != nil {
				id = *tt.args.resource.ID
			}
			got, err := svc.Name(ctx, tt.args.resource.Type, id)
			if (err != nil) != tt.wantErr {
				t.Errorf("Service.Name() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Service.Name() = %v, want %v", got, tt.want)
			}
		})
	}
}
