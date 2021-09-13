package query

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/flux"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
)

const (
	PreferHeaderKey                = "Prefer"
	PreferNoContentHeaderValue     = "return-no-content"
	PreferNoContentWErrHeaderValue = "return-no-content-with-error"
)

// Request represents the query to run.
// Options to mutate the header associated to this Request can be specified
// via `WithOption` or associated methods.
// One should always `Request.ApplyOptions()` before encoding and sending the request.
type Request struct {
	// Scope
	Authorization  *platform.Authorization `json:"authorization,omitempty"`
	OrganizationID platform2.ID            `json:"organization_id"`

	// Command

	// Compiler converts the query to a specification to run against the data.
	Compiler flux.Compiler `json:"compiler"`

	// Source represents the ultimate source of the request.
	Source string `json:"source"`

	// compilerMappings maps compiler types to creation methods
	compilerMappings flux.CompilerMappings

	options []RequestHeaderOption
}

// SetReturnNoContent sets the header for a Request to return no content.
func SetReturnNoContent(header http.Header, withError bool) {
	if withError {
		header.Set(PreferHeaderKey, PreferNoContentWErrHeaderValue)
	} else {
		header.Set(PreferHeaderKey, PreferNoContentHeaderValue)
	}
}

// RequestHeaderOption is a function that mutates the header associated to a Request.
type RequestHeaderOption = func(header http.Header) error

// WithOption adds a RequestHeaderOption to this Request.
func (r *Request) WithOption(option RequestHeaderOption) {
	r.options = append(r.options, option)
}

// WithReturnNoContent makes this Request return no content.
func (r *Request) WithReturnNoContent(withError bool) {
	r.WithOption(func(header http.Header) error {
		SetReturnNoContent(header, withError)
		return nil
	})
}

// ApplyOptions applies every option added to this Request to the given header.
func (r *Request) ApplyOptions(header http.Header) error {
	for _, visitor := range r.options {
		if err := visitor(header); err != nil {
			return err
		}
	}
	return nil
}

// WithCompilerMappings sets the query type mappings on the request.
func (r *Request) WithCompilerMappings(mappings flux.CompilerMappings) {
	r.compilerMappings = mappings
}

// UnmarshalJSON populates the request from the JSON data.
// WithCompilerMappings must have been called or an error will occur.
func (r *Request) UnmarshalJSON(data []byte) error {
	type Alias Request
	raw := struct {
		*Alias
		CompilerType flux.CompilerType `json:"compiler_type"`
		Compiler     json.RawMessage   `json:"compiler"`
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	createCompiler, ok := r.compilerMappings[raw.CompilerType]
	if !ok {
		return fmt.Errorf("unsupported compiler type %q", raw.CompilerType)
	}

	c := createCompiler()
	if err := json.Unmarshal(raw.Compiler, c); err != nil {
		return err
	}
	r.Compiler = c

	return nil
}

func (r Request) MarshalJSON() ([]byte, error) {
	type Alias Request
	raw := struct {
		Alias
		CompilerType flux.CompilerType `json:"compiler_type"`
	}{
		Alias:        (Alias)(r),
		CompilerType: r.Compiler.CompilerType(),
	}
	return json.Marshal(raw)
}

type contextKey struct{}

var activeContextKey = contextKey{}

// ContextWithRequest returns a new context with a reference to the request.
func ContextWithRequest(ctx context.Context, req *Request) context.Context {
	return context.WithValue(ctx, activeContextKey, req)
}

// RequestFromContext retrieves a *Request from a context.
// If not request exists on the context nil is returned.
func RequestFromContext(ctx context.Context) *Request {
	v := ctx.Value(activeContextKey)
	if v == nil {
		return nil
	}
	return v.(*Request)
}

// ProxyRequest specifies a query request and the dialect for the results.
type ProxyRequest struct {
	// Request is the basic query request
	Request Request `json:"request"`

	// Dialect is the result encoder
	Dialect flux.Dialect `json:"dialect"`

	// dialectMappings maps dialect types to creation methods
	dialectMappings flux.DialectMappings
}

// WithCompilerMappings sets the compiler type mappings on the request.
func (r *ProxyRequest) WithCompilerMappings(mappings flux.CompilerMappings) {
	r.Request.WithCompilerMappings(mappings)
}

// WithDialectMappings sets the dialect type mappings on the request.
func (r *ProxyRequest) WithDialectMappings(mappings flux.DialectMappings) {
	r.dialectMappings = mappings
}

// UnmarshalJSON populates the request from the JSON data.
// WithCompilerMappings and WithDialectMappings must have been called or an error will occur.
func (r *ProxyRequest) UnmarshalJSON(data []byte) error {
	type Alias ProxyRequest
	raw := struct {
		*Alias
		DialectType flux.DialectType `json:"dialect_type"`
		Dialect     json.RawMessage  `json:"dialect"`
	}{
		Alias: (*Alias)(r),
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	createDialect, ok := r.dialectMappings[raw.DialectType]
	if !ok {
		return fmt.Errorf("unsupported dialect type %q", raw.DialectType)
	}

	d := createDialect()
	if err := json.Unmarshal(raw.Dialect, d); err != nil {
		return err
	}
	r.Dialect = d

	return nil
}

func (r ProxyRequest) MarshalJSON() ([]byte, error) {
	type Alias ProxyRequest
	raw := struct {
		Alias
		DialectType flux.DialectType `json:"dialect_type"`
	}{
		Alias:       (Alias)(r),
		DialectType: r.Dialect.DialectType(),
	}
	return json.Marshal(raw)
}
