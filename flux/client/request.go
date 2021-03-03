package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb/query"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Controller interface {
	Query(ctx context.Context, compiler flux.Compiler) (flux.Query, error)
	PrometheusCollectors() []prometheus.Collector
}

// QueryRequest is a flux query request.
type QueryRequest struct {
	Type  string `json:"type"`
	Query string `json:"query"`

	// Flux fields
	Extern  json.RawMessage `json:"extern,omitempty"`
	AST     json.RawMessage `json:"ast,omitempty"`
	Dialect QueryDialect    `json:"dialect"`
	Now     time.Time       `json:"now"`

	// PreferNoContent specifies if the Response to this request should
	// contain any result. This is done for avoiding unnecessary
	// bandwidth consumption in certain cases. For example, when the
	// query produces side effects and the results do not matter. E.g.:
	// 	from(...) |> ... |> to()
	// For example, tasks do not use the results of queries, but only
	// care about their side effects.
	// To obtain a QueryRequest with no result, add the header
	// `Prefer: return-no-content` to the HTTP request.
	PreferNoContent bool
	// PreferNoContentWithError is the same as above, but it forces the
	// Response to contain an error if that is a Flux runtime error encoded
	// in the response body.
	// To obtain a QueryRequest with no result but runtime errors,
	// add the header `Prefer: return-no-content-with-error` to the HTTP request.
	PreferNoContentWithError bool
}

// QueryDialect is the formatting options for the query response.
type QueryDialect struct {
	Header         *bool    `json:"header"`
	Delimiter      string   `json:"delimiter"`
	CommentPrefix  string   `json:"commentPrefix"`
	DateTimeFormat string   `json:"dateTimeFormat"`
	Annotations    []string `json:"annotations"`
}

// WithDefaults adds default values to the request.
func (r QueryRequest) WithDefaults() QueryRequest {
	if r.Type == "" {
		r.Type = "flux"
	}
	if r.Dialect.Delimiter == "" {
		r.Dialect.Delimiter = ","
	}
	if r.Dialect.DateTimeFormat == "" {
		r.Dialect.DateTimeFormat = "RFC3339"
	}
	if r.Dialect.Header == nil {
		header := true
		r.Dialect.Header = &header
	}
	return r
}

// Validate checks the query request and returns an error if the request is invalid.
func (r QueryRequest) Validate() error {
	if r.Query == "" && r.AST == nil {
		return errors.New(`request body requires either query or AST`)
	}

	if r.Type != "flux" {
		return fmt.Errorf(`unknown query type: %s`, r.Type)
	}

	if len(r.Dialect.CommentPrefix) > 1 {
		return fmt.Errorf("invalid dialect comment prefix: must be length 0 or 1")
	}

	if len(r.Dialect.Delimiter) != 1 {
		return fmt.Errorf("invalid dialect delimeter: must be length 1")
	}

	rn, size := utf8.DecodeRuneInString(r.Dialect.Delimiter)
	if rn == utf8.RuneError && size == 1 {
		return fmt.Errorf("invalid dialect delimeter character")
	}

	for _, a := range r.Dialect.Annotations {
		switch a {
		case "group", "datatype", "default":
		default:
			return fmt.Errorf(`unknown dialect annotation type: %s`, a)
		}
	}

	switch r.Dialect.DateTimeFormat {
	case "RFC3339", "RFC3339Nano":
	default:
		return fmt.Errorf(`unknown dialect date time format: %s`, r.Dialect.DateTimeFormat)
	}

	return nil
}

// ProxyRequest specifies a query request and the dialect for the results.
type ProxyRequest struct {
	// Compiler converts the query to a specification to run against the data.
	Compiler flux.Compiler

	// Dialect is the result encoder
	Dialect flux.Dialect
}

// ProxyRequest returns a request to proxy from the flux.
func (r QueryRequest) ProxyRequest() *ProxyRequest {
	n := r.Now
	if n.IsZero() {
		n = time.Now()
	}

	// Query is preferred over spec
	var compiler flux.Compiler
	if r.Query != "" {
		compiler = lang.FluxCompiler{
			Query:  r.Query,
			Extern: r.Extern,
			Now:    n,
		}
	} else if len(r.AST) > 0 {
		c := lang.ASTCompiler{
			Extern: r.Extern,
			AST:    r.AST,
			Now:    n,
		}
		compiler = c
	}

	delimiter, _ := utf8.DecodeRuneInString(r.Dialect.Delimiter)

	noHeader := false
	if r.Dialect.Header != nil {
		noHeader = !*r.Dialect.Header
	}

	var dialect flux.Dialect
	if r.PreferNoContent {
		dialect = &query.NoContentDialect{}
	} else {
		// TODO(nathanielc): Use commentPrefix and dateTimeFormat
		// once they are supported.
		encConfig := csv.ResultEncoderConfig{
			NoHeader:    noHeader,
			Delimiter:   delimiter,
			Annotations: r.Dialect.Annotations,
		}
		if r.PreferNoContentWithError {
			dialect = &query.NoContentWithErrorDialect{
				ResultEncoderConfig: encConfig,
			}
		} else {
			dialect = &csv.Dialect{
				ResultEncoderConfig: encConfig,
			}
		}
	}

	return &ProxyRequest{
		Compiler: compiler,
		Dialect:  dialect,
	}
}
