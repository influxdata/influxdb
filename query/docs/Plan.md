# Plan DAG

The plan is represented as a DAG, where each node performs an operation and produces a result.
The plan DAG is separate and distinct from the query DAG.
The plan DAG specifies details about how the query will be executed, while the query DAG only specifies what the query is.

There may be multiple roots to the DAG where each root represents a source of data.
A root or source node may retrieve data from multiple different systems.
Primarily data will be read from the storage interface, but may be streamed from the write ingest system or potentially external systems as well.

The leaves of the DAG represent the results of the operation.
The results are collected and returned.

