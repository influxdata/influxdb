# Query DAG

The query is represented as a DAG, where each node represents an operation to be performed.

There may be multiple roots to the DAG where each root represents a source of data.
Root nodes will specify whether they are selecting data from the database or consuming data as a stream.

The leaves of the DAG represent the results of the operation.
A result node may be added as a child to any node to make that intermediate representation a finalized result.

## Specification

A query DAG consists of a set of nodes and a set of edges that form a directed acyclic graph (DAG).

Each node has the following properties:

* ID - A unique identifier for the node within the graph.
* Kind - The kind of operation the node performs.
* Spec - The spec, specifies the parameters provided to the node detailing the specifics of the operation.
    The parameters vary by the kind of node.


