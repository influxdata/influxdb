package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
)

func TestRolesStore_Get(t *testing.T) {
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    *chronograf.Role
		wantErr bool
	}{
		{
			name: "Role not found",
			args: args{
				ctx:  context.Background(),
				name: "unknown",
			},
			wantErr: true,
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

		s := client.RolesStore
		got, err := s.Get(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. RolesStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !cmp.Equal(got, tt.want) {
			t.Errorf("%q. RolesStore.Get() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
