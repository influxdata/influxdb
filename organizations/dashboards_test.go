package organizations_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/organizations"
)

// IgnoreFields is used because ID is created by BoltDB and cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var dashboardCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Dashboard{}, "ID"),
}

func TestDashboards_All(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Dashboard
		wantRaw  []chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name:    "No Dashboards",
			wantErr: true,
		},
		{
			name: "All Dashboards",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
			},
			want: []chronograf.Dashboard{
				{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			wantRaw: []chronograf.Dashboard{
				{
					Name:         "howdy",
					Organization: "1337",
				},
				{
					Name:         "doody",
					Organization: "1338",
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

		s := organizations.NewDashboardsStore(client.DashboardsStore)
		if tt.addFirst {
			for _, d := range tt.wantRaw {
				client.DashboardsStore.Add(tt.args.ctx, d)
			}
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], dashboardCmpOptions...); diff != "" {
				t.Errorf("%q. DashboardsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestDashboards_Add(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name    string
		args    args
		want    chronograf.Dashboard
		wantErr bool
	}{
		{
			name: "Add Dashbaord",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					Name: "howdy",
				},
			},
			want: chronograf.Dashboard{
				Name:         "howdy",
				Organization: "1337",
			},
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

		s := organizations.NewDashboardsStore(client.DashboardsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.dashboard)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, dashboardCmpOptions...); diff != "" {
			t.Errorf("%q. DashboardsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestDashboards_Delete(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete dashboard",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					Name:         "howdy",
					Organization: "1337",
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

		s := client.DashboardsStore
		if tt.addFirst {
			tt.args.dashboard, _ = client.DashboardsStore.Add(tt.args.ctx, tt.args.dashboard)
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Delete(tt.args.ctx, tt.args.dashboard)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestDashboards_Get(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Dashbaord",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Dashboard{
				Name:         "howdy",
				Organization: "1337",
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

		if tt.addFirst {
			tt.args.dashboard, _ = client.DashboardsStore.Add(tt.args.ctx, tt.args.dashboard)
		}
		s := organizations.NewDashboardsStore(client.DashboardsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.dashboard.ID)
		if diff := cmp.Diff(got, tt.want, dashboardCmpOptions...); diff != "" {
			t.Errorf("%q. DashboardsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestDashboards_Update(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
		name         string
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Dashbaord Name",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					Name:         "howdy",
					Organization: "1337",
				},
				name: "doody",
			},
			want: chronograf.Dashboard{
				Name:         "doody",
				Organization: "1337",
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

		if tt.addFirst {
			tt.args.dashboard, _ = client.DashboardsStore.Add(tt.args.ctx, tt.args.dashboard)
		}
		if tt.args.name != "" {
			tt.args.dashboard.Name = tt.args.name
		}
		s := organizations.NewDashboardsStore(client.DashboardsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Update(tt.args.ctx, tt.args.dashboard)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.dashboard.ID)
		if diff := cmp.Diff(got, tt.want, dashboardCmpOptions...); diff != "" {
			t.Errorf("%q. DashboardsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
