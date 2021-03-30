package influxdb

import (
	"context"

	"github.com/influxdata/influxdb/v2/kit/platform"
)

// ErrScraperTargetNotFound is the error msg for a missing scraper target.
const ErrScraperTargetNotFound = "scraper target not found"

// ops for ScraperTarget Store
const (
	OpListTargets   = "ListTargets"
	OpAddTarget     = "AddTarget"
	OpGetTargetByID = "GetTargetByID"
	OpRemoveTarget  = "RemoveTarget"
	OpUpdateTarget  = "UpdateTarget"
)

// ScraperTarget is a target to scrape
type ScraperTarget struct {
	ID            platform.ID `json:"id,omitempty"`
	Name          string      `json:"name"`
	Type          ScraperType `json:"type"`
	URL           string      `json:"url"`
	OrgID         platform.ID `json:"orgID,omitempty"`
	BucketID      platform.ID `json:"bucketID,omitempty"`
	AllowInsecure bool        `json:"allowInsecure,omitempty"`
}

// ScraperTargetStoreService defines the crud service for ScraperTarget.
type ScraperTargetStoreService interface {
	ListTargets(ctx context.Context, filter ScraperTargetFilter) ([]ScraperTarget, error)
	AddTarget(ctx context.Context, t *ScraperTarget, userID platform.ID) error
	GetTargetByID(ctx context.Context, id platform.ID) (*ScraperTarget, error)
	RemoveTarget(ctx context.Context, id platform.ID) error
	UpdateTarget(ctx context.Context, t *ScraperTarget, userID platform.ID) (*ScraperTarget, error)
}

// ScraperTargetFilter represents a set of filter that restrict the returned results.
type ScraperTargetFilter struct {
	IDs   map[platform.ID]bool `json:"ids"`
	Name  *string              `json:"name"`
	OrgID *platform.ID         `json:"orgID"`
	Org   *string              `json:"org"`
}

// ScraperType defines the scraper methods.
type ScraperType string

// Scraper types
const (
	// PrometheusScraperType parses metrics from a prometheus endpoint.
	PrometheusScraperType = "prometheus"
)

// ValidScraperType returns true is the type string is valid
func ValidScraperType(s string) bool {
	switch s {
	case PrometheusScraperType:
		return true
	default:
		return false
	}
}
