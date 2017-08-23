package kapacitor

import (
	"sync"

	client "github.com/influxdata/kapacitor/client/v1"
)

const (
	// ListTaskWorkers describes the number of workers concurrently fetching
	// tasks from Kapacitor. This constant was chosen after some benchmarking
	// work and should likely work well for quad-core systems
	ListTaskWorkers = 4

	// TaskGatherers is the number of workers collating responses from
	// ListTaskWorkers. There can only be one without additional synchronization
	// around the output buffer from ListTasks
	TaskGatherers = 1
)

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

	go p.generateKapacitorOptions(optChan, *opts, done)

	var wg sync.WaitGroup

	wg.Add(ListTaskWorkers)
	for i := 0; i < ListTaskWorkers; i++ {
		go p.fetchFromKapacitor(optChan, &wg, &once, taskChan, done)
	}

	var gatherWg sync.WaitGroup
	gatherWg.Add(TaskGatherers)
	go func() {
		for task := range taskChan {
			allTasks = append(allTasks, task...)
		}
		gatherWg.Done()
	}()

	wg.Wait()
	close(taskChan)
	gatherWg.Wait()

	return allTasks, nil
}

// fetchFromKapacitor fetches a set of results from a kapacitor by reading a
// set of options from the provided optChan. Fetched tasks are pushed onto the
// provided taskChan
func (p *PaginatingKapaClient) fetchFromKapacitor(optChan chan client.ListTasksOptions, wg *sync.WaitGroup, closer *sync.Once, taskChan chan []client.Task, done chan struct{}) {
	defer wg.Done()
	for opt := range optChan {
		resp, err := p.KapaClient.ListTasks(&opt)
		if err != nil {
			return
		}

		// break and stop all workers if we're done
		if len(resp) == 0 {
			closer.Do(func() {
				close(done)
			})
			return
		}

		// handoff tasks to consumer
		taskChan <- resp
	}
}

// generateKapacitorOptions creates ListTasksOptions with incrementally greater
// Limit and Offset parameters, and inserts them into the provided optChan
func (p *PaginatingKapaClient) generateKapacitorOptions(optChan chan client.ListTasksOptions, opts client.ListTasksOptions, done chan struct{}) {
	// ensure Limit and Offset start from known quantities
	opts.Limit = p.FetchRate
	opts.Offset = 0

	for {
		select {
		case <-done:
			close(optChan)
			return
		case optChan <- opts:
			// nop
		}
		opts.Offset = p.FetchRate + opts.Offset
	}
}
