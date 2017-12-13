package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
)

func TestConfig_Get(t *testing.T) {
	type wants struct {
		config *chronograf.Config
		err    error
	}
	tests := []struct {
		name  string
		wants wants
	}{
		{
			name: "Get config",
			wants: wants{
				config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminFirstUserOnly: false,
					},
				},
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

		s := client.ConfigStore
		got, err := s.Get(context.Background())
		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. ConfigStore.Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}
		if diff := cmp.Diff(got, tt.wants.config); diff != "" {
			t.Errorf("%q. ConfigStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestConfig_Update(t *testing.T) {
	type args struct {
		config *chronograf.Config
	}
	type wants struct {
		config *chronograf.Config
		err    error
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "Set config",
			args: args{
				config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminFirstUserOnly: true,
					},
				},
			},
			wants: wants{
				config: &chronograf.Config{
					Auth: chronograf.AuthConfig{
						SuperAdminFirstUserOnly: true,
					},
				},
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

		s := client.ConfigStore
		err = s.Update(context.Background(), tt.args.config)
		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. ConfigStore.Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}

		got, _ := s.Get(context.Background())
		if (tt.wants.err != nil) != (err != nil) {
			t.Errorf("%q. ConfigStore.Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
			continue
		}

		if diff := cmp.Diff(got, tt.wants.config); diff != "" {
			t.Errorf("%q. ConfigStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
