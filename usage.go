package influxdb

import (
	"context"
	"time"
)

// UsageMetric used to track classes of usage.
type UsageMetric string

const (
	// UsageWriteRequestCount is the name of the metrics for tracking write request count.
	UsageWriteRequestCount UsageMetric = "usage_write_request_count"
	// UsageWriteRequestBytes is the name of the metrics for tracking the number of write bytes.
	UsageWriteRequestBytes UsageMetric = "usage_write_request_bytes"

	// UsageValues is the name of the metrics for tracking the number of values.
	UsageValues UsageMetric = "usage_values"
	// UsageSeries is the name of the metrics for tracking the number of series written.
	UsageSeries UsageMetric = "usage_series"

	// UsageQueryRequestCount is the name of the metrics for tracking query request count.
	UsageQueryRequestCount UsageMetric = "usage_query_request_count"
	// UsageQueryRequestBytes is the name of the metrics for tracking the number of query bytes.
	UsageQueryRequestBytes UsageMetric = "usage_query_request_bytes"
)

// Usage is a metric associated with the utilization of a particular resource.
type Usage struct {
	OrganizationID *ID         `json:"organizationID,omitempty"`
	BucketID       *ID         `json:"bucketID,omitempty"`
	Type           UsageMetric `json:"type"`
	Value          float64     `json:"value"`
}

// UsageService is a service for accessing usage statistics.
type UsageService interface {
	GetUsage(ctx context.Context, filter UsageFilter) (map[UsageMetric]*Usage, error)
}

// UsageFilter is used to filter usage.
type UsageFilter struct {
	OrgID    *ID
	BucketID *ID
	Range    *Timespan
}

// Timespan represents a range of time.
type Timespan struct {
	Start time.Time `json:"start"`
	Stop  time.Time `json:"stop"`
}
