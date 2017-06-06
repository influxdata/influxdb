# Query Engine Internals

The query engine contains the processing framework for reading data
from the storage engine and processing it to produce data.

## Overview

The lifecycle of a query looks like this:

1. The InfluxQL query string is tokenized and then parsed into an abstract syntax
   tree (AST). This is the code representation of the query itself.
2. The query AST is compiled into a preliminary directed acyclical graph by
   the compiler. The query itself is validated and a directed acyclical graph
   is created for each field within the query. Any symbols (variables) that
   are encountered are recorded and will be resolved in the next stage. This
   stage is isolated and does not require any information from the storage
   engine or meta client.
3. The shard mapper is used to retrieve the shards from the storage engine that
   will be used by the query. Symbols are resolved to a real data type, wildcards
   are expanded, and type validation happens. This stage requires access to the
   storage engine and meta client. The full directed acyclical graph is constructed
   to be executed in the future.
4. The directed acyclical graph is processed. Each node processes its dependencies
   and then executes the creation of its own iterator using those dependencies.
5. The iterators for the leaf edges are processed as the outputs of the query.
   The emitter aggregates these points, matches them by time/name/tags, and then
   creates rows that get sent to the httpd service to be processed in whichever way
   is desirable.

## Nodes and Edges

The directed acyclical graph is composed of nodes and edges. Each node
represents some discrete action to be done on the input iterators.
Commonly, this means creating a new iterator to read and process the
inputs.

Each node can have zero or more inputs and at least one output. Nodes
are passed around as an interface.

An edge is composed of two parts: the read and write edge. The read edge
is the part of the edge meant to be used for reading the iterator that
was produced by a node. The write edge is the end where a node writes
to an iterator. So if you were to draw it as a directed acyclical graph,
you would end up with something like this:

    write ------> read

An edge is always composed of a write and read edge. Each side of the
edge will usually be associated with a node. A read edge at the end of
the graph (a leaf node) will not be associated with a node, but will
just be read from directly.

The read and write edge are split into two separate components to make it
easier to manipulate the graph. When connecting an edge to a node, an
attribute has to be set on both the node and the edge. If we were to move
an edge to point to a different node, we would have to modify the underlying
node for both sides of the edge. Since the edges are separate, we do not
need to modify the other side of the edge to modify an edge when we
insert, append, or remove an edge related to another node.

## Phases

Compilation and execution of a query is divided into multiple phases.

### Compilation

Compilation deals with creating the initial directed acyclical graph,
setting up the symbol table, and verifying the construction of the query.
The compilation step has **no access** to the underlying storage engine.
It is self-contained to the query subsystem.

Compilation begins by compiling the global state from the statement
abstract syntax tree. This includes retrieving the query dimensions,
the interval, parsing the condition and the time range from the where
clause. It also ensures the condition and time range are valid and
separated from each other so other portions of the query engine do not
have to worry about ignoring the time comparisons from their own logic.
This step also sets any missing time conditions to their defaults.

After it compiles the global portions of the statement, it compiles
a partial directed acyclical graph for each field. The partial graph
contains a chain of actions to be used when constructing the iterator,
but the final portion of the chain, the source of the iterator, remains
unassigned and the write edge at the beginning of the chain gets input
into the symbol table.

The symbol table is a reference to all of the unresolved write edges
that need to be constructed while linking the graph to the storage
engine for execution.

If a wildcard expression is encountered, a wildcard symbol is registered
in the symbol table and the compiled graph keeps track that the wildcard
will need to be resolved during linking.

The partial graph for each field retains annotations with references
to points of interest within the graph that are needed for validation
and/or linking.

### Linking

Linking deals with resolving symbols from compilation and linking the
different partial graphs from the various fields to a single graph that
can be executed by the executor.

During this phase, the query engine has access to the shard mapper.
The shard mapper can be used to request shard access from the database.
The linker uses the shard mapper to open the shards needed to create
an iterator for each measurement source referenced by the statement.

If a field has a wildcard, the linker duplicates the partial graph and
the symbol table for the field for each matching field/tag. It then
chooses a single field or tag for each of these created graphs and it
substitutes all wildcards with the same field or tag.

Wildcards are not resolved using combinations of different expansions.
If you were to add two wildcards together and the wildcard were to
expand to `v1` and `v2`, you would end up with only `v1 + v1` and
`v2 + v2`. You would not have `v1 + v2` or `v2 + v1`. Because of these
expansion rules, the most restrictive combination of wildcard rules
within a field determines the wildcard expansion.

During compilation, we had no access to information about the types
of each field. During linking, we now have access to the storage engine
and can therefore resolve the types for each field or tag during symbol
resolution. After constructing the graph, we verify that the types
for each node make sense so we can detect if a person is trying to use
a type with an aggregate that does not accept that type such as
attempting to use a tag or string with a call to `mean()`.

### Optimization

Optimization is done by manipulating the directed acyclical graph.
At first, a query plan will construct the simplest method of resolving
the query. For example, in the following query we might see:

    > SELECT count(value) FROM cpu
    shard [1] -\
    shard [2] ----> merge --> count()
    shard [3] -/

We can then optimize this merge. We know that `count()` can be used as
a partial aggregate. We can insert a `count()` function call between
each shard and the merge node and then modify the original `count()`
to be a `sum()`. The final query plan would look like this:

    shard [1] --> count() --\
    shard [2] --> count() ----> merge --> sum()
    shard [3] --> count() --/

Once we have generated the graph, we pass it to the executor to create
the iterators so the data can be processed.

### Execution

Query execution is done using the returned graph. Execution is very
similar to how a build system might invoke itself. A node waits for its
dependencies to be constructed and then it uses the output of those
dependencies to construct its own iterators. It then signals to its
outputs that they should be built if they are ready.

After this is finished, the iterators created by the leaf nodes are then
read by the emitter. Points that are at the same time are combined into
the same row and output to the calling service.
