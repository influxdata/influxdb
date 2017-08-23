package kapacitor_test

import (
	"testing"

	"github.com/influxdata/chronograf/kapacitor"
	"github.com/influxdata/chronograf/mocks"
	client "github.com/influxdata/kapacitor/client/v1"
)

func BenchmarkKapaClient100(b *testing.B)    { benchmark_PaginatingKapaClient(100, b) }
func BenchmarkKapaClient1000(b *testing.B)   { benchmark_PaginatingKapaClient(1000, b) }
func BenchmarkKapaClient10000(b *testing.B)  { benchmark_PaginatingKapaClient(10000, b) }
func BenchmarkKapaClient100000(b *testing.B) { benchmark_PaginatingKapaClient(100000, b) }

var tasks []client.Task

func benchmark_PaginatingKapaClient(taskCount int, b *testing.B) {

	b.StopTimer() // eliminate setup time

	// create a mock client that will return a huge response from ListTasks
	mockClient := &mocks.KapaClient{
		ListTasksF: func(opts *client.ListTasksOptions) ([]client.Task, error) {
			// create all the tasks
			allTasks := make([]client.Task, taskCount)

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

	b.StartTimer() // eliminate setup time

	// let the benchmark runner run ListTasks until it's satisfied
	for n := 0; n < b.N; n++ {
		// assignment is to avoid having the call optimized away
		tasks, _ = pkap.ListTasks(opts)
	}
}
