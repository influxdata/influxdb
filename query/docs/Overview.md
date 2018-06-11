# Design Overview

This document provides an overview of the design of the query engine.

## Concepts

There are several different concepts that make up the complete query engine.

* Query - A query defines work to be performed on time series data and a result.
    A query is represented as a directed acyclic graph (DAG).
* Flux - Functional Language for defining a query to execute.
* Parser - Parses a Flux script and produces a query.
* Data Frame - A data frame is a matrix of time series data where one dimension is time and the other is series.
* Query Node - A query node represents a single step in the DAG of a query.
* Planner - The planner creates a plan of execution from a query.
* Plan - A plan is also a DAG of node the explicitly state how a query will be performed.
* Plan Node - A plan node represents a single step in the DAG of a plan.
* Executor - The executor is responsible for processing a query plan.
    The executor process data via data frames.
* Storage - The Storage interface provides a mechanism for accessing the underlying data as data frames.
* Capabilities - The Storage interface exposes its capabilities.
    The planner uses the available capabilities to determine the best plan.
* Hints - The Storage interface exposes hints about the data.
    The planner uses the hints to determine the best plan.
* Query Engine - Query Engine is the name given to the entire system being described in this document.


Both a query and a plan are represented by a DAG and describe an operation that needs to be performed.
The difference is that a plan in addition to describing what the operation is, also describes how that operation will be performed.
In short, a query describes what the operation is and a plan describes how that operation will be carried out.

## Relations

Below is a high level description, using the Go language, of the relations between the different components and concepts of the query engine.

```go

type Parser interface {
    Parse(flux string) Query
}

// Query defines work to be performed on time series data and a result.
// A query is represented as a directed acyclic graph (DAG).
type Query interface {
    Nodes() []QueryNode
    Edges() []Edge
}

// QueryNode is a single step in the DAG of a query
type QueryNode interface {
    ID() NodeID
    // More details about what the node does
}

// NodeID uniquely identifies a node.
type NodeID

// Edge establishes a parent child relationship between two nodes.
type Edge interface {
    Parent() NodeID
    Child() NodeID
}

// Planner computes a plan from a query and available storage interfaces
type Planner interface {
    Plan(Query, []Storage) Plan
}

// Plan is a DAG of the specific steps to execute.
type Plan interface  {
    Nodes() []PlanNode
    Edges() []Edge
}

// PlanNode is a single step in the plan DAG.
type PlanNode interface {
    ID() NodeID
    Predicates() []Predicate
}

// Predicate filters data.
type Predicate interface {}

// Storage provides an interface to the storage layer.
type Storage interface {
    // Read gets data from the underlying storage system and returns a data frame or error state.
    Read(context.Context, []Predicate, TimeRange, Grouping) (DataFrame, ErrorState)
    // Capabilities exposes the capabilities of the storage interface.
    Capabilities() []Capability
    // Hints provides hints about the characteristics of the data.
    Hints(context.Context, []Predicate, TimeRange, Grouping) Hints
}

// TimeRange is the beginning time and ending time
type TimeRange interface {
    Begin() int64
    End() int64
}

// Grouping are key groups
type Grouping interface {
    Keys() []string
}

// Hints provide insight into the size and shape of the data that would likely be returned
// from a storage read operation.
type Hints interface {
    Cardinality() // Count tag values
    ByteSize()   int64
    Blocks()     int64
}

// Capability represents a single capability of a storage interface.
type Capability interface{
    Name() string
}

// Executor processes a plan and returns the resulting data frames or an error state.
type Executor interface{
    Execute(context.Context, Plan) ([]DataFrame, ErrorState)
}

// ErrorState describes precisely the state of an errored operation such that appropraite recovery may be attempted.
type ErrorState interface {
    Error() error
    OtherInformation()
    // Retryable() bool ?
}

```

