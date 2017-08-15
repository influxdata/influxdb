package kapacitor_test

import (
	"testing"

	"github.com/influxdata/chronograf/kapacitor"
	"github.com/influxdata/chronograf/mocks"
	client "github.com/influxdata/kapacitor/client/v1"
)

func Test_Kapacitor_PaginatingKapaClient(t *testing.T) {
	const lenAllTasks = 200

	// create a mock client that will return a huge response from ListTasks
	mockClient := &mocks.KapaClient{
		ListTasksF: func(opts *client.ListTasksOptions) ([]client.Task, error) {
			// create all the tasks
			allTasks := []client.Task{}
			for i := 0; i < lenAllTasks; i++ {
				allTasks = append(allTasks, client.Task{})
			}
			begin := opts.Offset
			end := opts.Offset + opts.Limit

			if end > len(allTasks) {
				end = len(allTasks)
			}

			return allTasks[begin:end], nil
		},
	}

	pkap := kapacitor.PaginatingKapaClient{mockClient, 100}

	opts := &client.ListTasksOptions{
		Limit:  100,
		Offset: 0,
	}

	// ensure 100 elems returned when calling mockClient directly
	tasks, _ := mockClient.ListTasks(opts)

	if len(tasks) != 100 {
		t.Error("Expected calling KapaClient's ListTasks to return", opts.Limit, "items. Received:", len(tasks))
	}

	// ensure PaginatingKapaClient returns _all_ tasks with 0 value for Limit and Offset
	allOpts := &client.ListTasksOptions{}
	allTasks, _ := pkap.ListTasks(allOpts)

	if len(allTasks) != lenAllTasks {
		t.Error("PaginatingKapaClient: Expected to find", lenAllTasks, "tasks but found", len(allTasks))
	}
}
