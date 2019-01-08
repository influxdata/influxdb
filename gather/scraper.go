package gather

import (
	"context"

	platform "github.com/influxdata/influxdb"
)

// Scraper gathers metrics from a scraper target.
type Scraper interface {
	Gather(ctx context.Context, target platform.ScraperTarget) (ms []Metrics, err error)
}
