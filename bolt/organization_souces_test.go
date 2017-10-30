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
var sourceCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Source{}, "ID"),
	cmpopts.IgnoreFields(chronograf.Source{}, "Default"),
}

func TestOrganizationSources_All(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Source
		wantRaw  []chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name:    "No Sources",
			wantErr: true,
		},
		{
			name: "All Sources",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
			},
			want: []chronograf.Source{
				{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			wantRaw: []chronograf.Source{
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

		s := client.OrganizationSourcesStore
		if tt.addFirst {
			for _, d := range tt.wantRaw {
				client.SourcesStore.Add(tt.args.ctx, d)
			}
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationSourcesStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], sourceCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationSourcesStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestOrganizationSources_Add(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name    string
		args    args
		want    chronograf.Source
		wantErr bool
	}{
		{
			name: "Add Source",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					Name: "howdy",
				},
			},
			want: chronograf.Source{
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

		s := client.OrganizationSourcesStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationSourcesStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationSourcesStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationSources_Delete(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete source",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
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

		s := client.OrganizationSourcesStore
		if tt.addFirst {
			tt.args.source, _ = client.SourcesStore.Add(tt.args.ctx, tt.args.source)
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Delete(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationSourcesStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestOrganizationSources_Get(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Source",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Source{
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
			tt.args.source, _ = client.SourcesStore.Add(tt.args.ctx, tt.args.source)
		}
		s := client.OrganizationSourcesStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationSourcesStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.source.ID)
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationSourcesStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationSources_Update(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
		name         string
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Source Name",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					Name:         "howdy",
					Organization: "1337",
				},
				name: "doody",
			},
			want: chronograf.Source{
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
			tt.args.source, _ = client.SourcesStore.Add(tt.args.ctx, tt.args.source)
		}
		if tt.args.name != "" {
			tt.args.source.Name = tt.args.name
		}
		s := client.OrganizationSourcesStore
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Update(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationSourcesStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.source.ID)
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationSourcesStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
