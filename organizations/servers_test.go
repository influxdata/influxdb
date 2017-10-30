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
var serverCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Server{}, "ID"),
	cmpopts.IgnoreFields(chronograf.Server{}, "Active"),
}

func TestServers_All(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Server
		wantRaw  []chronograf.Server
		addFirst bool
		wantErr  bool
	}{
		{
			name:    "No Servers",
			wantErr: true,
		},
		{
			name: "All Servers",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
			},
			want: []chronograf.Server{
				{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			wantRaw: []chronograf.Server{
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

		s := organizations.NewServersStore(client.ServersStore)
		if tt.addFirst {
			for _, d := range tt.wantRaw {
				client.ServersStore.Add(tt.args.ctx, d)
			}
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ServersStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], serverCmpOptions...); diff != "" {
				t.Errorf("%q. ServersStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestServers_Add(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		server       chronograf.Server
	}
	tests := []struct {
		name    string
		args    args
		want    chronograf.Server
		wantErr bool
	}{
		{
			name: "Add Server",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				server: chronograf.Server{
					Name: "howdy",
				},
			},
			want: chronograf.Server{
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

		s := organizations.NewServersStore(client.ServersStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.server)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ServersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, serverCmpOptions...); diff != "" {
			t.Errorf("%q. ServersStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestServers_Delete(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		server       chronograf.Server
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Server
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete server",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				server: chronograf.Server{
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

		s := organizations.NewServersStore(client.ServersStore)
		if tt.addFirst {
			tt.args.server, _ = client.ServersStore.Add(tt.args.ctx, tt.args.server)
		}
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Delete(tt.args.ctx, tt.args.server)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ServersStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestServers_Get(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		server       chronograf.Server
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Server
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Server",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				server: chronograf.Server{
					Name:         "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Server{
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
			tt.args.server, _ = client.ServersStore.Add(tt.args.ctx, tt.args.server)
		}
		s := organizations.NewServersStore(client.ServersStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ServersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.server.ID)
		if diff := cmp.Diff(got, tt.want, serverCmpOptions...); diff != "" {
			t.Errorf("%q. ServersStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestServers_Update(t *testing.T) {
	type args struct {
		organization string
		ctx          context.Context
		server       chronograf.Server
		name         string
	}
	tests := []struct {
		name     string
		args     args
		want     chronograf.Server
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Server Name",
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				server: chronograf.Server{
					Name:         "howdy",
					Organization: "1337",
				},
				name: "doody",
			},
			want: chronograf.Server{
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
			tt.args.server, _ = client.ServersStore.Add(tt.args.ctx, tt.args.server)
		}
		if tt.args.name != "" {
			tt.args.server.Name = tt.args.name
		}
		s := organizations.NewServersStore(client.ServersStore)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err = s.Update(tt.args.ctx, tt.args.server)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. ServersStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.server.ID)
		if diff := cmp.Diff(got, tt.want, serverCmpOptions...); diff != "" {
			t.Errorf("%q. ServersStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
