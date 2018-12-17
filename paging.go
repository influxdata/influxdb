package platform

import (
	"strconv"
)

const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// PagingFilter represents a filter containing url query params.
type PagingFilter interface {
	// QueryParams returns a map containing url query params.
	QueryParams() map[string][]string
}

// PagingLinks represents paging links.
type PagingLinks struct {
	Prev string `json:"prev,omitempty"`
	Self string `json:"self"`
	Next string `json:"next,omitempty"`
}

// FindOptions represents options passed to all find methods with multiple results.
type FindOptions struct {
	Limit      int
	Offset     int
	SortBy     string
	Descending bool
}

// QueryParams returns a map containing url query params.
func (f FindOptions) QueryParams() map[string][]string {
	qp := map[string][]string{
		"limit":      []string{strconv.Itoa(f.Limit)},
		"offset":     []string{strconv.Itoa(f.Offset)},
		"sortBy":     []string{f.SortBy},
		"descending": []string{strconv.FormatBool(f.Descending)},
	}

	return qp
}
