# Meta-Queries

This document describes design options for meta-queries.  Meta-queries are queries
or statements that apply to the data about points and series stored in the
system.  This could be what database exists, what users exists, the owners
of shards, etc.. Some examples statements include `SHOW MEASUREMENTS` and 
`DROP SERIES`.

# Context

In 0.10, the meta store has been logically separated into a remote service.  All
access to the meta store is through a meta client.  The meta store internally
uses raft to maintain consistency to all changes to the meta data about the
system.  

Previously, the meta store was tightly coupled to many parts of the system.
By separating it out and controlling access through the client, we can now run
dedicated meta nodes, data nodes or combined ones.

In a single node case or combined meta/data node, meta queries are fairly straight
forward because one node can generally answer all type of queries.  When nodes
are split, or data is not fully replicated, these operations become more complicated.

Meta-queries execute on the data nodes and may require changes to both the
meta store (mediated by meta nodes using raft) as well as operating on remote data
nodes and possibly its own local data.  Some meta-queries can be answered solely 
from the the local data nodes cache of the meta store data.  Others, require 
running a distributed query across all data nodes.

# Scenarios

There are a few categories of meta-queries we need to handle.  These scenarios
are used to evaluate different design options.

* Meta Data Only - The `SHOW DATABASES` meta query can be executed and handled
solely by the coordinating node's local cache.  The local cache describes all
databases, retention policies, shard groups, shards, CQs, authentication, and
subscriptions.  Any meta-query that needs to return any of this information
can be answered by the local cache.

* Data Store Only - The `SHOW SERIES` meta query needs to consult all data
nodes to have them return all series stored in their local shards.  Series
keys are not known to the meta store so a distributed query must run.

* Update Meta Data Only - The `DROP CONTINUOUS QUERY` statement can execute
directly on the meta nodes and delete the continuos queries stored in the meta
data.  When continuous queries execute, the worker node consults the meta 
store for the continous query to execute.

* Update Meta Data & Data Store - `DROP DATABASE` statement must execute on
the meta nodes to remove a database from the meta data and it must also
propogate the actual deletion of database data on data nodes.

* Update Data Store Only - `DROP SERIES` statement must executed on all data
nodes containing the series to be deleted.  The meta store has no knowledge of
series so it does not need to be update.

# Concerns

There are several system concerns and failure scenarios we need to handle. A
few are listed below:

* Partitioned Node - If a node is partitioned away or down and a data store
update query is executed such as a `DROP SERIES` statement.  How does the
design handle this case?  Does the coordinating node retry?  When the node
recover can it determine series where dropped? 

* Partial Success - If a data store update statement is executed and it fails 
on one node, how does the design handle this case?  Is the operation retried?
Is the user notified with an error?  If it's a query, does it return partial
success?


# Options

* Diff local state w/ meta-data cached state 
* Push (and queue) changes to remote 
* Add Change Log to meta.Data
* ...

# Discussion


