package query

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux"
	"github.com/influxdata/platform"
)

// Request respresents the query to run.
type Request struct {
	// Scope
	Authorization  *platform.Authorization `json:"authorization,omitempty"`
	OrganizationID platform.ID             `json:"organization_id"`

	// Command

	// Compiler converts the query to a specification to run against the data.
	Compiler flux.Compiler `json:"compiler"`

	// compilerMappings maps compiler types to creation methods
	compilerMappings flux.CompilerMappings
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

//RequestFromContext retrieves a *Request from a context.
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
