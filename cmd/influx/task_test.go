package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

//	Purpose of test suite:
//   	checking if cmd line tool gives all data needed for TaskService to perform functions
func TestCmdTask(t *testing.T) {
	orgID := platform.ID(9000)

	fakeSVCFn := func(svc taskmodel.TaskService) taskSVCsFn {
		return func() (taskmodel.TaskService, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: orgID, Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	t.Run("create", func(t *testing.T) {

		// todo: add more test cases
		tests := []struct {
			name         string
			expectedTask taskmodel.Task
			flags        []string
			envVars      map[string]string
		}{
			{
				name: "basic create",
				flags: []string{
					"--org=influxdata",
				},
				expectedTask: taskmodel.Task{
					OrganizationID: 9000,
					Organization:   "influxdata",
				},
			},
		}

		cmdFn := func(expectedTsk taskmodel.Task) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewTaskService()
			svc.CreateTaskFn = func(ctx context.Context, task taskmodel.TaskCreate) (*taskmodel.Task, error) {
				tmpTsk := taskmodel.Task{
					Type:           task.Type,
					OrganizationID: task.OrganizationID,
					Organization:   task.Organization,
					OwnerID:        task.OwnerID,
					Description:    task.Description,
					Status:         task.Status,
					Flux:           task.Flux,
				}

				errMsg := fmt.Errorf("unexpected task;\n\twant= %+v\n\tgot=  %+v", expectedTsk, task)
				if !cmp.Equal(expectedTsk, tmpTsk) {
					return nil, errMsg
				} else {
					return &expectedTsk, nil
				}
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdTaskBuilder(fakeSVCFn(svc), g, opt).cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expectedTask))
				cmd.SetArgs(append([]string{"task", "create"}, tt.flags...))

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}

	})

	// todo: add tests for task subcommands

}
