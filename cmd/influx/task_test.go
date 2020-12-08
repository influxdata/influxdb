package main

import (
	"context"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"testing"
)

func TestCmdTask(t *testing.T) {
	orgID := influxdb.ID(9000)

	fakeSVCFn := func(svc influxdb.TaskService) taskSVCFn {
		return func() (influxdb.TaskService, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: orgID, Name: "influxdata"}, nil
				},
			}, nil
		}
	}
}

//func Test_taskRerunFailedCmd(t *testing.T) {
//	type args struct {
//		f   *globalFlags
//		opt genericCLIOpts
//	}
//	tests := []struct {
//		name string
//		args args
//		want *cobra.Command
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := taskRerunFailedCmd(tt.args.f, tt.args.opt); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("taskRerunFailedCmd() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
