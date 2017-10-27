package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
)

// IgnoreFields is used because ID is created by BoltDB and cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var dashboardCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
}

func TestOrganizationDashboards_All(t *testing.T) {
	type args struct {
		organization string
	}
	tests := []struct {
		name     string
		ctx      context.Context
		args     args
		want     []chronograf.Dashboard
		wantRaw  []chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "No Dashboards",
		},
		{
			name: "All Dashbaords",
			args: {
				organization: "1337",
			},
			want: []chronograf.User{
				{
					Name:         "howdy",
					Provider:     "github",
					Scheme:       "oauth2",
					Organization: "1337",
					Roles: []chronograf.Role{
						{
							Name: "viewer",
						},
					},
				},
			},
			wantRaw: []chronograf.User{
				{
					Name:         "howdy",
					Provider:     "github",
					Scheme:       "oauth2",
					Organization: "1337",
					Roles: []chronograf.Role{
						{
							Name: "viewer",
						},
					},
				},
				{
					Name:         "doody",
					Provider:     "github",
					Scheme:       "oauth2",
					Organization: "1338",
					Roles: []chronograf.Role{
						{
							Name: "editor",
						},
					},
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationDashboards
		if tt.addFirst {
			for _, u := range tt.want {
				client.DashboardsStore.Add(tt.ctx, &u)
			}
		}
		gots, err := s.All(tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationDashboardsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], cmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationDashboardsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}
