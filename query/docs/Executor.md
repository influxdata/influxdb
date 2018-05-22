# Executor Design

This document lays out the design of the executor.

## Interface

The Executor interface is defined as:

```go
type Executor interface {
    Execute(context.Context, Plan) ([]Result, ErrorState)
}
```

The executor is responsible for taking a specific plan from the Planner and executing it to produce the result which is a list of Results will allow for streaming of the various results to a client.

##  Concepts

The executor interacts with many different systems and has its own internal systems
Below is a list of concepts within the executor.

| Concept         | Description                                                                                                                                                               |
| -------         | -----------                                                                                                                                                               |
| Bounded Data    | Datasets that are finite, in other words `batch` data.                                                                                                                    |
| Unbounded Data  | Datasets that have no know end, or are infinite, in other words `stream` data.                                                                                            |
| Event time      | The time the event actually occurred.                                                                                                                                     |
| Processing time | The time the event is processed. This time may be completely out of order with respect to its event time and the event time of other events with similar processing time. |
| Watermarks      | Watermarks communicate the lag between event time and processing time. Watermarks define a bound on the event time of data that has been observed.                        |
| Triggers        | Triggers communicate when data should be materialized.                                                                                                                    |
| Accumulation    | Accumulation defines how different results from events of the same window can be combined into a single result.                                                           |
| Dataset         | A set of data produced from a transformation. The dataset is resilient because its lineage and watermarks are known, therfore it can be recreated in the event of loss.   |
| Block           | A subset of a dataset. Row represent series and columns represent time.                                                                                                   |
| Transformation  | Performs a transformation on data received from a parent dataset and writes results to a child dataset.                                                                   |
| Execution State | Execution state tracks the state of an execution.                                                                                                                         |

## Execution State

While both queries and plans are specifications the execution state encapsulates the implementation and state of executing a query.

