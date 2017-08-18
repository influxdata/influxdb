package kapacitor

import (
	client "github.com/influxdata/kapacitor/client/v1"
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

	allOpts := &client.ListTasksOptions{
		// copy existing fields
		TaskOptions: opts.TaskOptions,
		Pattern:     opts.Pattern,
		Fields:      opts.Fields,

		// we take control of these two in the loop below
		Limit:  p.FetchRate,
		Offset: 0,
	}

	for {
		resp, err := p.KapaClient.ListTasks(allOpts)
		if err != nil {
			return allTasks, err
		}

		// break if we've exhausted available tasks
		if len(resp) == 0 {
			break
		}

		allTasks = append(allTasks, resp...)
		allOpts.Offset += len(resp)
	}
	return allTasks, nil
}
