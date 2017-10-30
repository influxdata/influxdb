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

// IgnoreFields is used because ID is created by BoltDB and cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var layoutCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Layout{}, "ID"),
}

func TestLayouts_All(t *testing.T) {
	type fields struct {
		LayoutsStore chronograf.LayoutsStore
	}
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    []chronograf.Layout
		wantRaw []chronograf.Layout
		wantErr bool
	}{
		{
			name: "No Layouts",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					AllF: func(ctx context.Context) ([]chronograf.Layout, error) {
						return nil, fmt.Errorf("No Layouts")
					},
				},
			},
			wantErr: true,
		},
		{
			name: "All Layouts",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					AllF: func(ctx context.Context) ([]chronograf.Layout, error) {
						return []chronograf.Layout{
							{
								Application:  "howdy",
								Organization: "1337",
							},
							{
								Application:  "doody",
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
			want: []chronograf.Layout{
				{
					Application:  "howdy",
					Organization: "1337",
				},
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewLayoutsStore(tt.fields.LayoutsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. LayoutsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], layoutCmpOptions...); diff != "" {
				t.Errorf("%q. LayoutsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestLayouts_Add(t *testing.T) {
	type fields struct {
		LayoutsStore chronograf.LayoutsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    chronograf.Layout
		wantErr bool
	}{
		{
			name: "Add Layout",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					AddF: func(ctx context.Context, s chronograf.Layout) (chronograf.Layout, error) {
						return s, nil
					},
					GetF: func(ctx context.Context, id string) (chronograf.Layout, error) {
						return chronograf.Layout{
							ID:           "1229",
							Application:  "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					ID:          "1229",
					Application: "howdy",
				},
			},
			want: chronograf.Layout{
				Application:  "howdy",
				Organization: "1337",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewLayoutsStore(tt.fields.LayoutsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. LayoutsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. LayoutsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestLayouts_Delete(t *testing.T) {
	type fields struct {
		LayoutsStore chronograf.LayoutsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     []chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete layout",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					DeleteF: func(ctx context.Context, s chronograf.Layout) error {
						return nil
					},
					GetF: func(ctx context.Context, id string) (chronograf.Layout, error) {
						return chronograf.Layout{
							ID:           "1229",
							Application:  "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					ID:           "1229",
					Application:  "howdy",
					Organization: "1337",
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		s := organizations.NewLayoutsStore(tt.fields.LayoutsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err := s.Delete(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. LayoutsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestLayouts_Get(t *testing.T) {
	type fields struct {
		LayoutsStore chronograf.LayoutsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Layout",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					GetF: func(ctx context.Context, id string) (chronograf.Layout, error) {
						return chronograf.Layout{
							ID:           "1229",
							Application:  "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					ID:           "1229",
					Application:  "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Layout{
				ID:           "1229",
				Application:  "howdy",
				Organization: "1337",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewLayoutsStore(tt.fields.LayoutsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		got, err := s.Get(tt.args.ctx, tt.args.layout.ID)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. LayoutsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. LayoutsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestLayouts_Update(t *testing.T) {
	type fields struct {
		LayoutsStore chronograf.LayoutsStore
	}
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
		name         string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Layout Application",
			fields: fields{
				LayoutsStore: &mocks.LayoutsStore{
					UpdateF: func(ctx context.Context, s chronograf.Layout) error {
						return nil
					},
					GetF: func(ctx context.Context, id string) (chronograf.Layout, error) {
						return chronograf.Layout{
							ID:           "1229",
							Application:  "doody",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					ID:           "1229",
					Application:  "howdy",
					Organization: "1337",
				},
				name: "doody",
			},
			want: chronograf.Layout{
				Application:  "doody",
				Organization: "1337",
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		if tt.args.name != "" {
			tt.args.layout.Application = tt.args.name
		}
		s := organizations.NewLayoutsStore(tt.fields.LayoutsStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err := s.Update(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. LayoutsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.layout.ID)
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. LayoutsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
