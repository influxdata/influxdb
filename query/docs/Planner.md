# Planner Design

This document lays out the design of the planner.

## Interface

The Planner inter is defined as:

```go
type Planner interface {
    Plan(Query, []Storage) Plan
}
```

The planner is responsible for taking a query DAG and a set of available storage interfaces and produce a plan DAG.

## Plans

Plans are created via a two step process:

1. Create a general plan from the query.
2. Create a specific plan from the general plan and available storage interface.

The general plan specifies all the needed data frames and their lineage needed to produce the final query result.
The specific plan specifies how the general plan will be executed, which storage interfaces will be consumed and how.

The general plan does not leave the scope of the Planner and is not part of the API of the planner.
Hence the Plan type above it the specific plan.

## Plan DAG

Both the general and specific plans are represented as DAGs.
The nodes of the DAG represent data frames to be produced, while the edges of the DAG represent the operations need to construct the data frames.
This is inverted from the Query DAG where the nodes are operations and edges represents data sets.

The leaves of the plan DAG represent sources of data and the data flows from bottom up through the tree.
Again this is inverted from the Query DAG where data flows top down.

## Data Frames

Data frames are a set of data and their lineage is known.
Meaning it is known what parent data frames and operations are needed to construct the data frame.
Using this concept of lineage allows a data frame to be reconstructed if it is loss due to node failure or if its parent data frames are modified.

### Windowing

Data frames will specify their windowing properties. ????

## Operations

Operations are a definition of a transformation to be applied on one data frame resulting in another.

### Narrow vs Wide

Operations are classified as either narrow or wide:

* Narrow operations map each parent data frame to exactly one child data frame.
    Specifically a narrow operation is a one-to-one mapping of parent to child data frames.
* Wide operations map multiple parent data frames to multiple child data frames.
    Specifically a wide operation is a many-to-many mapping of parent to child data frames.

This distinction is necessary to precisely define the lineage of a data frame.

