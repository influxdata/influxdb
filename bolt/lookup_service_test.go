package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/mock"
	platformtesting "github.com/influxdata/platform/testing"
)

var (
	testID    = platform.ID(1)
	testIDStr = testID.String()
)

func TestClient_Name(t *testing.T) {
	type initFn func(ctx context.Context, c *bolt.Client) error
	type args struct {
		resource platform.Resource
		id       platform.ID
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
				resource: platform.DashboardsResource,
				id:       platform.InvalidID(),
			},
			wantErr: true,
		},
		{
			name: "error if resource is invalid",
			args: args{
				resource: platform.Resource("invalid"),
			},
			wantErr: true,
		},
		{
			name: "authorization resource without a name returns empty string",
			args: args{
				resource: platform.AuthorizationsResource,
				id:       testID,
			},
			want: "",
		},
		{
			name: "task resource without a name returns empty string",
			args: args{
				resource: platform.TasksResource,
				id:       testID,
			},
			want: "",
		},

		{
			name: "bucket with existing id returns name",
			args: args{
				resource: platform.BucketsResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					_ = s.CreateOrganization(ctx, &platform.Organization{
						Name: "o1",
					})
					return s.CreateBucket(ctx, &platform.Bucket{
						Name:           "b1",
						OrganizationID: testID,
					})
				},
			},
			want: "b1",
		},
		{
			name: "bucket with non-existent id returns error",
			args: args{
				resource: platform.BucketsResource,
				id:       testID,
			},
			wantErr: true,
		},
		{
			name: "dashboard with existing id returns name",
			args: args{
				resource: platform.DashboardsResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateDashboard(ctx, &platform.Dashboard{
						Name: "dashboard1",
					})
				},
			},
			want: "dashboard1",
		},
		{
			name: "dashboard with non-existent id returns error",
			args: args{
				resource: platform.DashboardsResource,
				id:       testID,
			},
			wantErr: true,
		},
		{
			name: "org with existing id returns name",
			args: args{
				resource: platform.OrgsResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateOrganization(ctx, &platform.Organization{
						Name: "org1",
					})
				},
			},
			want: "org1",
		},
		{
			name: "org with non-existent id returns error",
			args: args{
				resource: platform.OrgsResource,
				id:       testID,
			},
			wantErr: true,
		},
		{
			name: "source with existing id returns name",
			args: args{
				resource: platform.SourcesResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateSource(ctx, &platform.Source{
						Name: "source1",
					})
				},
			},
			want: "source1",
		},
		{
			name: "source with non-existent id returns error",
			args: args{
				resource: platform.SourcesResource,
				id:       testID,
			},
			wantErr: true,
		},
		{
			name: "telegraf with existing id returns name",
			args: args{
				resource: platform.TelegrafsResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateTelegrafConfig(ctx, &platform.TelegrafConfig{
						OrganizationID: platformtesting.MustIDBase16("0000000000000009"),
						Name:           "telegraf1",
					}, testID)
				},
			},
			want: "telegraf1",
		},
		{
			name: "telegraf with non-existent id returns error",
			args: args{
				resource: platform.TelegrafsResource,
				id:       testID,
			},
			wantErr: true,
		},
		{
			name: "user with existing id returns name",
			args: args{
				resource: platform.UsersResource,
				id:       testID,
				init: func(ctx context.Context, s *bolt.Client) error {
					return s.CreateUser(ctx, &platform.User{
						Name: "user1",
					})
				},
			},
			want: "user1",
		},
		{
			name: "user with non-existent id returns error",
			args: args{
				resource: platform.UsersResource,
				id:       testID,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, done, err := NewTestClient()
			if err != nil {
				t.Fatalf("unable to create bolt test client: %v", err)
			}
			defer done()

			c.IDGenerator = mock.NewIDGenerator(testIDStr, t)
			ctx := context.Background()
			if tt.args.init != nil {
				if err := tt.args.init(ctx, c); err != nil {
					t.Errorf("Service.Name() unable to initialize service: %v", err)
				}
			}
			got, err := c.Name(ctx, tt.args.resource, tt.args.id)
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
