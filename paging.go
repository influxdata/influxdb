package influxdb

import (
	"net/url"
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
		"descending": {strconv.FormatBool(f.Descending)},
		"offset":     {strconv.Itoa(f.Offset)},
	}

	if f.Limit > 0 {
		qp["limit"] = []string{strconv.Itoa(f.Limit)}
	}

	if f.SortBy != "" {
		qp["sortBy"] = []string{f.SortBy}
	}

	return qp
}

// NewPagingLinks returns a PagingLinks.
// num is the number of returned results.
func NewPagingLinks(basePath string, opts FindOptions, f PagingFilter, num int) *PagingLinks {
	u := url.URL{
		Path: basePath,
	}

	values := url.Values{}
	for k, vs := range f.QueryParams() {
		for _, v := range vs {
			if v != "" {
				values.Add(k, v)
			}
		}
	}

	var self, next, prev string
	for k, vs := range opts.QueryParams() {
		for _, v := range vs {
			if v != "" {
				values.Add(k, v)
			}
		}
	}

	u.RawQuery = values.Encode()
	self = u.String()

	if num >= opts.Limit {
		nextOffset := opts.Offset + opts.Limit
		values.Set("offset", strconv.Itoa(nextOffset))
		u.RawQuery = values.Encode()
		next = u.String()
	}

	if opts.Offset > 0 {
		prevOffset := opts.Offset - opts.Limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		values.Set("offset", strconv.Itoa(prevOffset))
		u.RawQuery = values.Encode()
		prev = u.String()
	}

	links := &PagingLinks{
		Prev: prev,
		Self: self,
		Next: next,
	}

	return links
}
