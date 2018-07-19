package organizations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/organizations"
)

// IgnoreFields is used because ID cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var dashboardCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Dashboard{}, "ID"),
}

func TestDashboards_All(t *testing.T) {
	type fields struct {
		DashboardsStore chronograf.DashboardsStore
	}
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    []chronograf.Dashboard
		wantRaw []chronograf.Dashboard
		wantErr bool
	}{
		{
			name: "No Dashboards",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					AllF: func(ctx context.Context) ([]chronograf.Dashboard, error) {
						return nil, fmt.Errorf("No Dashboards")
					},
				},
			},
			wantErr: true,
		},
		{
			name: "All Dashboards",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					AllF: func(ctx context.Context) ([]chronograf.Dashboard, error) {
						return []chronograf.Dashboard{
							{
								Name:         "howdy",
								Organization: "1337",
							},
							{
								Name:         "doody",
								Organization: "1338",
							},
						}, nil
					},
				},
			},
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
		},
	}
	for _, tt := range tests {
		s := organizations.NewDashboardsStore(tt.fields.DashboardsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
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
	type fields struct {
		DashboardsStore chronograf.DashboardsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    chronograf.Dashboard
		wantErr bool
	}{
		{
			name: "Add Dashboard",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					AddF: func(ctx context.Context, s chronograf.Dashboard) (chronograf.Dashboard, error) {
						return s, nil
					},
					GetF: func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
						return chronograf.Dashboard{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					ID:   1229,
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
		s := organizations.NewDashboardsStore(tt.fields.DashboardsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
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
	type fields struct {
		DashboardsStore chronograf.DashboardsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     []chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete dashboard",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					DeleteF: func(ctx context.Context, s chronograf.Dashboard) error {
						return nil
					},
					GetF: func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
						return chronograf.Dashboard{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					ID:           1229,
					Name:         "howdy",
					Organization: "1337",
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		s := organizations.NewDashboardsStore(tt.fields.DashboardsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
		err := s.Delete(tt.args.ctx, tt.args.dashboard)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestDashboards_Get(t *testing.T) {
	type fields struct {
		DashboardsStore chronograf.DashboardsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Dashboard",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					GetF: func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
						return chronograf.Dashboard{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					ID:           1229,
					Name:         "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Dashboard{
				ID:           1229,
				Name:         "howdy",
				Organization: "1337",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewDashboardsStore(tt.fields.DashboardsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
		got, err := s.Get(tt.args.ctx, tt.args.dashboard.ID)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. DashboardsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, dashboardCmpOptions...); diff != "" {
			t.Errorf("%q. DashboardsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestDashboards_Update(t *testing.T) {
	type fields struct {
		DashboardsStore chronograf.DashboardsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		dashboard    chronograf.Dashboard
		name         string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Dashboard
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Dashboard Name",
			fields: fields{
				DashboardsStore: &mocks.DashboardsStore{
					UpdateF: func(ctx context.Context, s chronograf.Dashboard) error {
						return nil
					},
					GetF: func(ctx context.Context, id chronograf.DashboardID) (chronograf.Dashboard, error) {
						return chronograf.Dashboard{
							ID:           1229,
							Name:         "doody",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				dashboard: chronograf.Dashboard{
					ID:           1229,
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
		if tt.args.name != "" {
			tt.args.dashboard.Name = tt.args.name
		}
		s := organizations.NewDashboardsStore(tt.fields.DashboardsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
		err := s.Update(tt.args.ctx, tt.args.dashboard)
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
