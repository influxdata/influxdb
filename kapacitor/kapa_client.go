package kapacitor

import (
	"sync"

	client "github.com/influxdata/kapacitor/client/v1"
)

const ListTaskWorkers = 4

// ensure PaginatingKapaClient is a KapaClient
var _ KapaClient = &PaginatingKapaClient{}

// PaginatingKapaClient is a Kapacitor client that automatically navigates
// through Kapacitor's pagination to fetch all results
type PaginatingKapaClient struct {
	KapaClient
	FetchRate int // specifies the number of elements to fetch from Kapacitor at a time
}

// ListTasks lists all available tasks from Kapacitor, navigating pagination as
// it fetches them
func (p *PaginatingKapaClient) ListTasks(opts *client.ListTasksOptions) ([]client.Task, error) {
	// only trigger auto-pagination with Offset=0 and Limit=0
	if opts.Limit != 0 || opts.Offset != 0 {
		return p.KapaClient.ListTasks(opts)
	}

	allTasks := []client.Task{}

	optChan := make(chan client.ListTasksOptions)
	taskChan := make(chan []client.Task, ListTaskWorkers)
	done := make(chan struct{})

	var once sync.Once

	doneCloser := func() {
		close(done)
	}

	go func() {
		opts := &client.ListTasksOptions{
			// copy existing fields
			TaskOptions: opts.TaskOptions,
			Pattern:     opts.Pattern,
			Fields:      opts.Fields,

			// we take control of these two in the loop below
			Limit:  p.FetchRate,
			Offset: 0,
		}

		for {
			select {
			case <-done:
				close(optChan)
				return
			case optChan <- *opts:
				// nop
			}
			opts.Offset = p.FetchRate + opts.Offset
		}
	}()

	var wg sync.WaitGroup

	wg.Add(ListTaskWorkers)
	for i := 0; i < ListTaskWorkers; i++ {
		go func() {
			defer wg.Done()
			for opt := range optChan {
				resp, err := p.KapaClient.ListTasks(&opt)
				if err != nil {
					return
				}

				// break and stop all workers if we're done
				if len(resp) == 0 {
					once.Do(doneCloser)
					return
				}

				// handoff tasks to consumer
				taskChan <- resp
			}
		}()
	}

	var taskAsm sync.WaitGroup
	taskAsm.Add(1)
	go func() {
		for task := range taskChan {
			allTasks = append(allTasks, task...)
		}
		taskAsm.Done()
	}()

	wg.Wait()
	close(taskChan)
	taskAsm.Wait()

	return allTasks, nil
}
