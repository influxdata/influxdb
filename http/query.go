package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/query"
)

// QueryRequest is a flux query request.
type QueryRequest struct {
	Spec    *flux.Spec   `json:"spec,omitempty"`
	AST     *ast.Program `json:"ast,omitempty"`
	Query   string       `json:"query"`
	Type    string       `json:"type"`
	Dialect QueryDialect `json:"dialect"`

	org *platform.Organization
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
	if r.Query == "" && r.Spec == nil && r.AST == nil {
		return errors.New(`request body requires either query, spec, or AST`)
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

	rune, size := utf8.DecodeRuneInString(r.Dialect.Delimiter)
	if rune == utf8.RuneError && size == 1 {
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

func nowFunc(now time.Time) values.Function {
	timeVal := values.NewTime(values.ConvertTime(now))
	ftype := semantic.NewFunctionType(semantic.FunctionSignature{
		Return: semantic.Time,
	})
	call := func(args values.Object) (values.Value, error) {
		return timeVal, nil
	}
	sideEffect := false
	return values.NewFunction("now", ftype, call, sideEffect)
}

func toSpec(p *ast.Program, now func() time.Time) (*flux.Spec, error) {
	itrp := flux.NewInterpreter()
	itrp.SetOption("now", nowFunc(now()))
	semProg, err := semantic.New(p)
	if err != nil {
		return nil, err
	}

	if err := itrp.Eval(semProg); err != nil {
		return nil, err
	}
	return flux.ToSpec(itrp, itrp.SideEffects()...), nil
}

// ProxyRequest returns a request to proxy from the flux.
func (r QueryRequest) ProxyRequest() (*query.ProxyRequest, error) {
	return r.proxyRequest(time.Now)
}

func (r QueryRequest) proxyRequest(now func() time.Time) (*query.ProxyRequest, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	// Query is preferred over spec
	var compiler flux.Compiler
	if r.Query != "" {
		compiler = lang.FluxCompiler{
			Query: r.Query,
		}
	} else if r.AST != nil {
		var err error
		r.Spec, err = toSpec(r.AST, now)
		if err != nil {
			return nil, err
		}
		compiler = lang.SpecCompiler{
			Spec: r.Spec,
		}
	} else if r.Spec != nil {
		compiler = lang.SpecCompiler{
			Spec: r.Spec,
		}
	}

	delimiter, _ := utf8.DecodeRuneInString(r.Dialect.Delimiter)

	noHeader := false
	if r.Dialect.Header != nil {
		noHeader = !*r.Dialect.Header
	}

	// TODO(nathanielc): Use commentPrefix and dateTimeFormat
	// once they are supported.
	return &query.ProxyRequest{
		Request: query.Request{
			OrganizationID: r.org.ID,
			Compiler:       compiler,
		},
		Dialect: csv.Dialect{
			ResultEncoderConfig: csv.ResultEncoderConfig{
				NoHeader:    noHeader,
				Delimiter:   delimiter,
				Annotations: r.Dialect.Annotations,
			},
		},
	}, nil
}

// QueryRequestFromProxyRequest converts a query.ProxyRequest into a QueryRequest.
// The ProxyRequest must contain supported compilers and dialects otherwise an error occurs.
func QueryRequestFromProxyRequest(req *query.ProxyRequest) (*QueryRequest, error) {
	qr := new(QueryRequest)
	switch c := req.Request.Compiler.(type) {
	case lang.FluxCompiler:
		qr.Type = "flux"
		qr.Query = c.Query
	case lang.SpecCompiler:
		qr.Type = "flux"
		qr.Spec = c.Spec
	default:
		return nil, fmt.Errorf("unsupported compiler %T", c)
	}
	switch d := req.Dialect.(type) {
	case *csv.Dialect:
		var header = !d.ResultEncoderConfig.NoHeader
		qr.Dialect.Header = &header
		qr.Dialect.Delimiter = string(d.ResultEncoderConfig.Delimiter)
		qr.Dialect.CommentPrefix = "#"
		qr.Dialect.DateTimeFormat = "RFC3339"
		qr.Dialect.Annotations = d.ResultEncoderConfig.Annotations
	default:
		return nil, fmt.Errorf("unsupported dialect %T", d)
	}

	return qr, nil
}

func decodeQueryRequest(ctx context.Context, r *http.Request, svc platform.OrganizationService) (*QueryRequest, error) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return nil, err
	}

	req = req.WithDefaults()
	err := req.Validate()
	if err != nil {
		return nil, err
	}

	req.org, err = queryOrganization(ctx, r, svc)
	return &req, err
}

func decodeProxyQueryRequest(ctx context.Context, r *http.Request, auth *platform.Authorization, svc platform.OrganizationService) (*query.ProxyRequest, error) {
	req, err := decodeQueryRequest(ctx, r, svc)
	if err != nil {
		return nil, err
	}

	pr, err := req.ProxyRequest()
	if err != nil {
		return nil, err
	}

	pr.Request.Authorization = auth
	return pr, nil
}
