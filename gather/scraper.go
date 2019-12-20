package gather

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

// Scraper gathers metrics from a scraper target.
type Scraper interface {
	Gather(ctx context.Context, target influxdb.ScraperTarget) (collected MetricsCollection, err error)
}
