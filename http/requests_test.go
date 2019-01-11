package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

func Test_queryOrganization(t *testing.T) {
	type args struct {
		ctx context.Context
		r   *http.Request
		svc platform.OrganizationService
	}
	tests := []struct {
		name    string
		args    args
		want    *platform.Organization
		wantErr bool
	}{
		{
			name: "org id finds organization",
			want: &platform.Organization{
				ID: platform.ID(1),
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?orgID=0000000000000001", nil),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						if *filter.ID == platform.ID(1) {
							return &platform.Organization{
								ID: platform.ID(1),
							}, nil
						}
						return nil, &platform.Error{
							Code: platform.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
		{
			name:    "bad id returns error",
			wantErr: true,
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?orgID=howdy", nil),
			},
		},
		{
			name: "org name finds organization",
			want: &platform.Organization{
				ID:   platform.ID(1),
				Name: "org1",
			},
			args: args{
				ctx: context.Background(),
				r:   httptest.NewRequest(http.MethodPost, "/api/v2/query?org=org1", nil),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						if *filter.Name == "org1" {
							return &platform.Organization{
								ID:   platform.ID(1),
								Name: "org1",
							}, nil
						}
						return nil, &platform.Error{
							Code: platform.EInvalid,
							Msg:  "unknown org name",
						}
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryOrganization(tt.args.ctx, tt.args.r, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryOrganization() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryOrganization() = %v, want %v", got, tt.want)
			}
		})
	}
}
