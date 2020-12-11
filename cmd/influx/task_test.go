package main

import (
	"github.com/spf13/cobra"
	"reflect"
	"testing"
)

func Test_taskRerunFailedCmd(t *testing.T) {

	/*
		Need to:
		1. create a mock task backend
		2. create a task
		3. have it fail couple times
		4. run testrerun

		how to output a cobra.Command?
	*/
	type args struct {
		f   *globalFlags
		opt genericCLIOpts
	}
	tests := []struct {
		name string
		args args
		want *cobra.Command
	}{
		{
			name: "basic",
			args: args{
				f:   nil,
				opt: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := taskRerunFailedCmd(tt.args.f, tt.args.opt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("taskRerunFailedCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}
