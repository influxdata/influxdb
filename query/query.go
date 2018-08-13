// Package query contains the InfluxDB 2.0 query engine.
package query

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/influxdata/platform"
)

// QueryService represents a type capable of performing queries.
type QueryService interface {
	// Query submits a query for execution returning a results iterator.
	// Cancel must be called on any returned results to free resources.
	Query(ctx context.Context, req *Request) (ResultIterator, error)
}

// AsyncQueryService represents a service for performing queries where the results are delivered asynchronously.
type AsyncQueryService interface {
	// Query submits a query for execution returning immediately.
	// Done must be called on any returned Query objects.
	Query(ctx context.Context, req *Request) (Query, error)
}

// ProxyQueryService performs queries and encodes the result into a writer.
// The results are opaque to a ProxyQueryService.
type ProxyQueryService interface {
	// Query performs the requested query and encodes the results into w.
	// The number of bytes written to w is returned __independent__ of any error.
	Query(ctx context.Context, w io.Writer, req *ProxyRequest) (int64, error)
}

// ResultIterator allows iterating through all results
// Cancel must be called to free resources.
// ResultIterators may implement Statisticser.
type ResultIterator interface {
	// More indicates if there are more results.
	More() bool

	// Next returns the next result.
	// If More is false, Next panics.
	Next() Result

	// Cancel discards the remaining results.
	// Cancel must always be called to free resources.
	// It is safe to call Cancel multiple times.
	Cancel()

	// Err reports the first error encountered.
	// Err will not report anything unless More has returned false,
	// or the query has been cancelled.
	Err() error
}

// Query represents an active query.
type Query interface {
	// Spec returns the spec used to execute this query.
	// Spec must not be modified.
	Spec() *Spec

	// Ready returns a channel that will deliver the query results.
	// Its possible that the channel is closed before any results arrive,
	// in which case the query should be inspected for an error using Err().
	Ready() <-chan map[string]Result

	// Done must always be called to free resources.
	Done()

	// Cancel will stop the query execution.
	// Done must still be called to free resources.
	// It is safe to call Cancel multiple times.
	Cancel()

	// Err reports any error the query may have encountered.
	Err() error

	Statisticser
}

// Request respresents the query to run.
type Request struct {
	// Scope
	Authorization  *platform.Authorization `json:"authorization,omitempty"`
	OrganizationID platform.ID             `json:"organization_id"`

	// Command

	// Compiler converts the query to a specification to run against the data.
	Compiler Compiler `json:"compiler"`

	// compilerMappings maps compiler types to creation methods
	compilerMappings CompilerMappings
}

// WithCompilerMappings sets the query type mappings on the request.
func (r *Request) WithCompilerMappings(mappings CompilerMappings) {
	r.compilerMappings = mappings
}

// UnmarshalJSON populates the request from the JSON data.
// WithCompilerMappings must have been called or an error will occur.
func (r *Request) UnmarshalJSON(data []byte) error {
	type Alias Request
	raw := struct {
		*Alias
		CompilerType CompilerType    `json:"compiler_type"`
		Compiler     json.RawMessage `json:"compiler"`
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
		CompilerType CompilerType `json:"compiler_type"`
	}{
		Alias:        (Alias)(r),
		CompilerType: r.Compiler.CompilerType(),
	}
	return json.Marshal(raw)
}

// Compiler produces a specification for the query.
type Compiler interface {
	// Compile produces a specification for the query.
	Compile(ctx context.Context) (*Spec, error)
	CompilerType() CompilerType
}

// CompilerType is the name of a query compiler.
type CompilerType string
type CreateCompiler func() Compiler
type CompilerMappings map[CompilerType]CreateCompiler

func (m CompilerMappings) Add(t CompilerType, c CreateCompiler) error {
	if _, ok := m[t]; ok {
		return fmt.Errorf("duplicate compiler mapping for %q", t)
	}
	m[t] = c
	return nil
}

// ProxyRequest specifies a query request and the dialect for the results.
type ProxyRequest struct {
	// Request is the basic query request
	Request Request `json:"request"`

	// Dialect is the result encoder
	Dialect Dialect `json:"dialect"`

	// dialectMappings maps dialect types to creation methods
	dialectMappings DialectMappings
}

// WithCompilerMappings sets the compiler type mappings on the request.
func (r *ProxyRequest) WithCompilerMappings(mappings CompilerMappings) {
	r.Request.WithCompilerMappings(mappings)
}

// WithDialectMappings sets the dialect type mappings on the request.
func (r *ProxyRequest) WithDialectMappings(mappings DialectMappings) {
	r.dialectMappings = mappings
}

// UnmarshalJSON populates the request from the JSON data.
// WithCompilerMappings and WithDialectMappings must have been called or an error will occur.
func (r *ProxyRequest) UnmarshalJSON(data []byte) error {
	type Alias ProxyRequest
	raw := struct {
		*Alias
		DialectType DialectType     `json:"dialect_type"`
		Dialect     json.RawMessage `json:"dialect"`
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
		DialectType DialectType `json:"dialect_type"`
	}{
		Alias:       (Alias)(r),
		DialectType: r.Dialect.DialectType(),
	}
	return json.Marshal(raw)
}

// Dialect describes how to encode results.
type Dialect interface {
	// Encoder creates an encoder for the results
	Encoder() MultiResultEncoder
	// DialectType report the type of the dialect
	DialectType() DialectType
}

// DialectType is the name of a query result dialect.
type DialectType string
type CreateDialect func() Dialect

type DialectMappings map[DialectType]CreateDialect

func (m DialectMappings) Add(t DialectType, c CreateDialect) error {
	if _, ok := m[t]; ok {
		return fmt.Errorf("duplicate dialect mapping for %q", t)
	}
	m[t] = c
	return nil
}

// Statisticser reports statisitcs about query processing.
type Statisticser interface {
	// Statistics reports the statisitcs for the query.
	// The statisitcs are not complete until the query is finished.
	Statistics() Statistics
}

// Statistics is a collection of statisitcs about the processing of a query.
type Statistics struct {
	// TotalDuration is the total amount of time in nanoseconds spent.
	TotalDuration time.Duration `json:"total_duration"`
	// CompileDuration is the amount of time in nanoseconds spent compiling the query.
	CompileDuration time.Duration `json:"compile_duration"`
	// QueueDuration is the amount of time in nanoseconds spent queueing.
	QueueDuration time.Duration `json:"queue_duration"`
	// PlanDuration is the amount of time in nanoseconds spent in plannig the query.
	PlanDuration time.Duration `json:"plan_duration"`
	// RequeueDuration is the amount of time in nanoseconds spent requeueing.
	RequeueDuration time.Duration `json:"requeue_duration"`
	// ExecuteDuration is the amount of time in nanoseconds spent in executing the query.
	ExecuteDuration time.Duration `json:"execute_duration"`

	// Concurrency is the number of goroutines allocated to process the query
	Concurrency int `json:"concurrency"`
	// MaxAllocated is the maximum number of bytes the query allocated.
	MaxAllocated int64 `json:"max_allocated"`
}
