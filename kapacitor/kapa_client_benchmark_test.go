package kapacitor_test

import (
	"testing"

	"github.com/influxdata/chronograf/kapacitor"
	"github.com/influxdata/chronograf/mocks"
	client "github.com/influxdata/kapacitor/client/v1"
)

func BenchmarkKapaClient100(b *testing.B)   { benchmark_PaginatingKapaClient(100, b) }
func BenchmarkKapaClient1000(b *testing.B)  { benchmark_PaginatingKapaClient(1000, b) }
func BenchmarkKapaClient10000(b *testing.B) { benchmark_PaginatingKapaClient(10000, b) }

func benchmark_PaginatingKapaClient(taskCount int, b *testing.B) {
	// create a mock client that will return a huge response from ListTasks
	mockClient := &mocks.KapaClient{
		ListTasksF: func(opts *client.ListTasksOptions) ([]client.Task, error) {
			// create all the tasks
			allTasks := []client.Task{}

			// create N tasks from the benchmark runner
			for i := 0; i < taskCount; i++ {
				allTasks = append(allTasks, client.Task{})
			}
			begin := opts.Offset
			end := opts.Offset + opts.Limit

			if end > len(allTasks) {
				end = len(allTasks)
			}

			if begin > len(allTasks) {
				begin = end
			}

			return allTasks[begin:end], nil
		},
	}

	pkap := kapacitor.PaginatingKapaClient{mockClient, 50}

	opts := &client.ListTasksOptions{}

	// let the benchmark runner run ListTasks until it's satisfied
	for n := 0; n < b.N; n++ {
		_, _ = pkap.ListTasks(opts)
	}
}
