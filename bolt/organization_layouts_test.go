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
var layoutCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Layout{}, "ID"),
}

func TestOrganizationLayouts_All(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Layout
		wantRaw  []chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name:    "No Layouts",
			wantErr: true,
		},
		{
			name: "All Layouts",
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
			wantRaw: []chronograf.Layout{
				{
					Application:  "howdy",
					Organization: "1337",
				},
				{
					Application:  "doody",
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

		s := client.OrganizationLayoutsStore
		if tt.addFirst {
			for _, d := range tt.wantRaw {
				client.LayoutsStore.Add(tt.args.ctx, d)
			}
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationLayoutsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], layoutCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationLayoutsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestOrganizationLayouts_Add(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name    string
		args    args
		want    chronograf.Layout
		wantErr bool
	}{
		{
			name: "Add Layout",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
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
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationLayoutsStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationLayoutsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationLayoutsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationLayouts_Delete(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete layout",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					Application:  "howdy",
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

		s := client.OrganizationLayoutsStore
		if tt.addFirst {
			tt.args.layout, _ = client.LayoutsStore.Add(tt.args.ctx, tt.args.layout)
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Delete(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationLayoutsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestOrganizationLayouts_Get(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Layout",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					Application:  "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Layout{
				Application:  "howdy",
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
			tt.args.layout, _ = client.LayoutsStore.Add(tt.args.ctx, tt.args.layout)
		}
		s := client.OrganizationLayoutsStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationLayoutsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.layout.ID)
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationLayoutsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationLayouts_Update(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		layout       chronograf.Layout
		application  string
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Layout
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Layout Application",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				layout: chronograf.Layout{
					Application:  "howdy",
					Organization: "1337",
				},
				application: "doody",
			},
			want: chronograf.Layout{
				Application:  "doody",
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
			tt.args.layout, _ = client.LayoutsStore.Add(tt.args.ctx, tt.args.layout)
		}
		if tt.args.application != "" {
			tt.args.layout.Application = tt.args.application
		}
		s := client.OrganizationLayoutsStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Update(tt.args.ctx, tt.args.layout)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationLayoutsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.layout.ID)
		if diff := cmp.Diff(got, tt.want, layoutCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationLayoutsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
