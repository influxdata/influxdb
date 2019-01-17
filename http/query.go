package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/semantic"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

// QueryRequest is a flux query request.
type QueryRequest struct {
	Spec    *flux.Spec   `json:"spec,omitempty"`
	AST     *ast.Package `json:"ast,omitempty"`
	Query   string       `json:"query"`
	Type    string       `json:"type"`
	Dialect QueryDialect `json:"dialect"`

	Org *platform.Organization `json:"-"`
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

// QueryAnalysis is a structured response of errors.
type QueryAnalysis struct {
	Errors []queryParseError `json:"errors"`
}

type queryParseError struct {
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	Character int    `json:"character"`
	Message   string `json:"message"`
}

// Analyze attempts to parse the query request and returns any errors
// encountered in a structured way.
func (r QueryRequest) Analyze() (*QueryAnalysis, error) {
	switch r.Type {
	case "flux":
		return r.analyzeFluxQuery()
	case "influxql":
		return r.analyzeInfluxQLQuery()
	}

	return nil, fmt.Errorf("unknown query request type %s", r.Type)
}

func (r QueryRequest) analyzeFluxQuery() (*QueryAnalysis, error) {
	a := &QueryAnalysis{}
	pkg := parser.ParseSource(r.Query)
	errCount := ast.Check(pkg)
	if errCount == 0 {
		a.Errors = []queryParseError{}
		return a, nil
	}
	a.Errors = make([]queryParseError, 0, errCount)
	ast.Walk(ast.CreateVisitor(func(node ast.Node) {
		loc := node.Location()
		for _, err := range node.Errs() {
			a.Errors = append(a.Errors, queryParseError{
				Line:    loc.Start.Line,
				Column:  loc.Start.Column,
				Message: err.Msg,
			})
		}
	}), pkg)
	return a, nil
}

func (r QueryRequest) analyzeInfluxQLQuery() (*QueryAnalysis, error) {
	a := &QueryAnalysis{}
	_, err := influxql.ParseQuery(r.Query)
	if err == nil {
		a.Errors = []queryParseError{}
		return a, nil
	}

	ms := influxqlParseErrorRE.FindAllStringSubmatch(err.Error(), -1)
	a.Errors = make([]queryParseError, 0, len(ms))
	for _, m := range ms {
		if len(m) != 4 {
			return nil, fmt.Errorf("influxql query error is not formatted as expected: got %d matches expected 4", len(m))
		}
		msg := m[1]
		lineStr := m[2]
		line, err := strconv.Atoi(lineStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line number from error mesage: %s -> %v", lineStr, err)
		}
		charStr := m[3]
		char, err := strconv.Atoi(charStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse character number from error mesage: %s -> %v", charStr, err)
		}

		a.Errors = append(a.Errors, queryParseError{
			Line:      line,
			Column:    columnFromCharacter(r.Query, char),
			Character: char,
			Message:   msg,
		})
	}

	return a, nil
}

func columnFromCharacter(q string, char int) int {
	col := 0
	for i, c := range q {
		if c == '\n' {
			col = 0
		}

		if i == char {
			break
		}
		col++
	}

	return col
}

var influxqlParseErrorRE = regexp.MustCompile(`^(.+) at line (\d+), char (\d+)$`)

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

func toSpec(p *ast.Package, now func() time.Time) (*flux.Spec, error) {
	semProg, err := semantic.New(p)
	if err != nil {
		return nil, err
	}

	scope := flux.Prelude()
	scope.Set("now", nowFunc(now()))

	itrp := interpreter.NewInterpreter()

	sideEffects, err := itrp.Eval(semProg, scope, flux.StdLib())
	if err != nil {
		return nil, err
	}

	nowOpt, ok := scope.Lookup("now")
	if !ok {
		return nil, fmt.Errorf("now option not set")
	}

	nowTime, err := nowOpt.Function().Call(nil)
	if err != nil {
		return nil, err
	}

	return flux.ToSpec(sideEffects, nowTime.Time().Time())
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
			OrganizationID: r.Org.ID,
			Compiler:       compiler,
		},
		Dialect: &csv.Dialect{
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

	var contentType = "application/json"
	if ct := r.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}
	switch mt {
	case "application/vnd.flux":
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		req.Query = string(body)
	case "application/json":
		fallthrough
	default:
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nil, err
		}
	}

	req = req.WithDefaults()
	if err := req.Validate(); err != nil {
		return nil, err
	}

	req.Org, err = queryOrganization(ctx, r, svc)
	return &req, err
}

func decodeProxyQueryRequest(ctx context.Context, r *http.Request, auth platform.Authorizer, svc platform.OrganizationService) (*query.ProxyRequest, error) {
	req, err := decodeQueryRequest(ctx, r, svc)
	if err != nil {
		return nil, err
	}

	pr, err := req.ProxyRequest()
	if err != nil {
		return nil, err
	}

	a, ok := auth.(*platform.Authorization)
	if !ok {
		// TODO(desa): this should go away once we're using platform.Authorizers everywhere.
		return pr, platform.ErrAuthorizerNotSupported
	}

	pr.Request.Authorization = a
	return pr, nil
}
