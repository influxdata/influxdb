package migration

import (
	"context"
	"strings"

	"github.com/influxdata/chronograf/bolt"
)

// changeIntervalToDuration
// Before, we supported queries that included `GROUP BY :interval:`
// After, we only support queries with `GROUP BY time(:interval:)`
// thereby allowing non_negative_derivative(_____, :interval)
var changeIntervalToDuration = Migration{
	ID:   "59b0cda4fc7909ff84ee5c4f9cb4b655b6a26620",
	Up:   up,
	Down: down,
}

var up = func(ctx context.Context, client bolt.Client) error {
	boards, err := client.DashboardsStore.All(ctx)
	if err != nil {
		return err
	}

	for _, board := range boards {
		for i, cell := range board.Cells {
			for i, query := range cell.Queries {
				query.Command = strings.Replace(query.Command, ":interval:", "time(:interval:)", -1)
				cell.Queries[i] = query
			}

			board.Cells[i] = cell
		}

		client.DashboardsStore.Update(ctx, board)
	}

	return nil
}

var down = func(ctx context.Context, client bolt.Client) error {
	return nil
}
