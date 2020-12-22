package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

//func Test_taskRerunFailedCmd(t *testing.T) {
//
//	/*
//		Need to:
//		1. create a mock task backend
//		2. create a task
//		3. have it fail couple times
//		4. run testrerun
//
//		how to output a cobra.Command?
//	*/
//}

func TestCmdTask(t *testing.T) {
	orgID := influxdb.ID(9000)

	fakeSVCFn := func(svc influxdb.TaskService) taskSVCsFn {
		return func() (influxdb.TaskService, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: orgID, Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	t.Run("create", func(t *testing.T) {
		/*
			checking cmd line tool gives all data needed for TaskService to actually create a Task
		*/
		tests := []struct {
			name         string
			expectedTask influxdb.Task
			flags        []string
			envVars      map[string]string
		}{
			{
				name: "basic create",
				flags: []string{
					"--org=influxdata",
				},
				expectedTask: influxdb.Task{
					OrganizationID: 9000,
					Organization:   "influxdata",
				},
			},
		}

		cmdFn := func(expectedTsk influxdb.Task) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewTaskService()

			//todo: Task vs TaskCreate?
			svc.CreateTaskFn = func(ctx context.Context, task influxdb.TaskCreate) (*influxdb.Task, error) {
				tmpTsk := influxdb.Task{
					Type:           task.Type,
					OrganizationID: task.OrganizationID,
					Organization:   task.Organization,
					OwnerID:        task.OwnerID,
					Description:    task.Description,
					Status:         task.Status,
					Flux:           task.Flux,
				}

				errMsg := fmt.Errorf("unexpected task;\n\twant= %+v\n\tgot=  %+v", expectedTsk, task)

				// todo: compare fields for expected to actual/"tmpTsk"
				if expectedTsk.Type != tmpTsk.Type {
					return nil, errMsg
				} else if expectedTsk.Flux != tmpTsk.Flux {
					return nil, errMsg
				} else if expectedTsk.Status != tmpTsk.Status {
					return nil, errMsg
				} else if expectedTsk.Description != tmpTsk.Description {
					return nil, errMsg
				} else if expectedTsk.Organization != tmpTsk.Organization {
					return nil, errMsg
				} else if expectedTsk.OrganizationID != tmpTsk.OrganizationID {
					return nil, errMsg
				} else if expectedTsk.OwnerID != tmpTsk.OwnerID {
					return nil, errMsg
				} else {
					// todo: buckets doesn't require a bucket returned
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

	}) // end t.Run create

	//t.Run("RerunFailed", func(t *testing.T) {
	//	//	/*
	//	//		Need to:
	//	//		1. create a mock task backend
	//	//		2. create a task
	//	//		3. have it fail couple times
	//	//		4. run testrerun
	//	//
	//	//		how to output a cobra.Command?
	//	//	*/
	//	tests := struct {
	//		name         string
	//		expectedTask influxdb.Task
	//	}{}
	//
	//}) //end t.Run RerunFailed

}
