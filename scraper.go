package platform

import (
	"context"
)

// ScraperTarget is a target to scrape
type ScraperTarget struct {
	ID         ID          `json:"id,omitempty"`
	Name       string      `json:"name"`
	Type       ScraperType `json:"type"`
	URL        string      `json:"url"`
	OrgName    string      `json:"org"`
	BucketName string      `json:"bucket"`
}

// ScraperTargetStoreService defines the crud service for ScraperTarget.
type ScraperTargetStoreService interface {
	ListTargets(ctx context.Context) ([]ScraperTarget, error)
	AddTarget(ctx context.Context, t *ScraperTarget) error
	GetTargetByID(ctx context.Context, id ID) (*ScraperTarget, error)
	RemoveTarget(ctx context.Context, id ID) error
	UpdateTarget(ctx context.Context, t *ScraperTarget) (*ScraperTarget, error)
}

// ScraperTargetFilter represents a set of filter that restrict the returned results.
type ScraperTargetFilter struct {
	ID   *ID     `json:"id"`
	Name *string `json:"name"`
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
