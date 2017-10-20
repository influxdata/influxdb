package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
)

var orgCMPOptions = cmp.Options{
	cmpopts.EquateEmpty(),
}

func TestOrganizationsStore_Get(t *testing.T) {
	type args struct {
		ctx context.Context
		org *chronograf.Organization
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.Organization
		wantErr  bool
		addFirst bool
	}{
		{
			name: "Organization not found",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{},
			},
			wantErr: true,
		},
		{
			name: "Get Organization",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					Name: "EE - Evil Empire",
				},
			},
			want: &chronograf.Organization{
				Name: "EE - Evil Empire",
			},
			addFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTestClient()
			if err != nil {
				t.Fatal(err)
			}

			if err := client.Open(context.TODO()); err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			s := client.OrganizationsStore
			if tt.addFirst {
				tt.args.org, err = s.Add(tt.args.ctx, tt.args.org)
				if err != nil {
					t.Fatal(err)
				}
			}

			got, err := s.Get(tt.args.ctx, tt.args.org.Name)
			if (err != nil) != tt.wantErr {
				t.Errorf("%q. OrganizationsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, orgCMPOptions...); diff != "" {
				t.Errorf("%q. OrganizationsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}
