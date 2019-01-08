package http

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
)

// decodeFindOptions returns a FindOptions decoded from http request.
func decodeFindOptions(ctx context.Context, r *http.Request) (*platform.FindOptions, error) {
	opts := &platform.FindOptions{}
	qp := r.URL.Query()

	if offset := qp.Get("offset"); offset != "" {
		o, err := strconv.Atoi(offset)
		if err != nil {
			return nil, err
		}

		opts.Offset = o
	}

	if limit := qp.Get("limit"); limit != "" {
		l, err := strconv.Atoi(limit)
		if err != nil {
			return nil, err
		}

		if l < 1 || l > platform.MaxPageSize {
			return nil, errors.InvalidDataf("limit must be between 1 and %d", platform.MaxPageSize)
		}

		opts.Limit = l
	} else {
		opts.Limit = platform.DefaultPageSize
	}

	if sortBy := qp.Get("sortBy"); sortBy != "" {
		opts.SortBy = sortBy
	}

	if descending := qp.Get("descending"); descending != "" {
		desc, err := strconv.ParseBool(descending)
		if err != nil {
			return nil, err
		}

		opts.Descending = desc
	}

	return opts, nil
}

// newPagingLinks returns a PagingLinks.
// num is the number of returned results.
func newPagingLinks(basePath string, opts platform.FindOptions, f platform.PagingFilter, num int) *platform.PagingLinks {
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

	links := &platform.PagingLinks{
		Prev: prev,
		Self: self,
		Next: next,
	}

	return links
}
