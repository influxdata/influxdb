package influxdb

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
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
	After      *platform.ID
	SortBy     string
	Descending bool
}

// GetLimit returns the resolved limit between then limit boundaries.
// Given a limit <= 0 it returns the default limit.
func (f *FindOptions) GetLimit() int {
	if f == nil || f.Limit <= 0 {
		return DefaultPageSize
	}

	if f.Limit > MaxPageSize {
		return MaxPageSize
	}

	return f.Limit
}

// DecodeFindOptions returns a FindOptions decoded from http request.
func DecodeFindOptions(r *http.Request) (*FindOptions, error) {
	opts := &FindOptions{}
	qp := r.URL.Query()

	if offset := qp.Get("offset"); offset != "" {
		o, err := strconv.Atoi(offset)
		if err != nil {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "offset is invalid",
			}
		}

		opts.Offset = o
	}

	if after := qp.Get("after"); after != "" {
		id, err := platform.IDFromString(after)
		if err != nil {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Err:  fmt.Errorf("decoding after: %w", err),
			}
		}

		opts.After = id
	}

	if limit := qp.Get("limit"); limit != "" {
		l, err := strconv.Atoi(limit)
		if err != nil {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "limit is invalid",
			}
		}

		if l < 1 || l > MaxPageSize {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("limit must be between 1 and %d", MaxPageSize),
			}
		}

		opts.Limit = l
	} else {
		opts.Limit = DefaultPageSize
	}

	if sortBy := qp.Get("sortBy"); sortBy != "" {
		opts.SortBy = sortBy
	}

	if descending := qp.Get("descending"); descending != "" {
		desc, err := strconv.ParseBool(descending)
		if err != nil {
			return nil, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "descending is invalid",
			}
		}

		opts.Descending = desc
	}

	return opts, nil
}

func FindOptionParams(opts ...FindOptions) [][2]string {
	var out [][2]string
	for _, o := range opts {
		for k, vals := range o.QueryParams() {
			for _, v := range vals {
				out = append(out, [2]string{k, v})
			}
		}
	}
	return out
}

// QueryParams returns a map containing url query params.
func (f FindOptions) QueryParams() map[string][]string {
	qp := map[string][]string{
		"descending": {strconv.FormatBool(f.Descending)},
		"offset":     {strconv.Itoa(f.Offset)},
	}

	if f.After != nil {
		qp["after"] = []string{f.After.String()}
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
