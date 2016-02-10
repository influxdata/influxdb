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

* Local vs. Cluster Query - The `SHOW SERIES` meta query should have two 
versions; one which returns all series in the queried database, and another
which returns only the series located on the specified node. Presumably the
meta queries which execute against data nodes should have a `SERVER =` clause
to allow direct inquiry of that node's data, not the entire cluster.

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
  * Measurements and series aren't stored in the current state, and it would be
    expensive to do so, so offline nodes would miss `DROP SERIES` commands
* Push (and queue) changes to remote
  * In order to guarantee execution, the queue would need to be replicated, at
    which point the third option becomes favorable
* Add Change Log to meta.Data
  * Nodes already poll for meta store changes, queries will be replicated (with
    consensus), and nodes can replay missed queries

# Proposed Design

Add a field to the `meta.Data` struct that will track a list queries that need
to be run on data nodes. Three items need to be tracked for each query:

 - A monotonically increasing ID (probably uint64, starting at 0) that can be
   used by clients to track which queries have already been run
 - A timestamp when the query was added to the log. This doesn't need to be
   extremely precise or synchronized, and would only be used for expriring
   entries from the log on a coarse time-scale (maybe a week to starti, probably
   make it configurable).
 - The query to be executed.

Each data node would be required to track one additional value: the highest ID
it has processed from the above described log entries. This will allow the node,
while it is starting up, to replay the missed entries and make an attempt to
syncronize its data with the expected state it should be in.

Here are some thoughts, reasonings, and potential issues using this approach:

 - Executing a query that requires the above described logging would be a matter
   of communicating with a meta node and acknowledging that the query was added
   to the log. The data nodes are already polling for changes in the meta
   service's data, and would then apploy the query on those updates.
 - As mentioned briefly above, the query log in this design would be replicated
   via Raft consensus. This is preferable to making a single data node (the
   recipient of the initial query) responsible for queueing the query for
   offline/unavailable nodes.
 - The number of queries that will need to be logged and tracked for offline
   nodes is very low during normal operations (mostly dropping/creating
   databases/retention policies and dropping measurements/series), so storing
   a list of the recent queries shouldn't increate the metadata blob size
   unreasonably
 - A potential problem would be a drop/create database in quick succession with
   points that get queued on other nodes' HH queues before the drop, but aren't
   received until the node comes back online.
    - One potential solution would be to assign every database (or retention
      policy) a UUID, while still maintaining at most one database per name. The
      writes would then be directed at either the old or new UUID, instead of
      the user-friendly name.
 - Expiring old entries would be done after sufficient time has been given for
   offline/partitioned nodes to come back online. One week is probably more than
   sufficient, as healthy nodes will mostly likely be up-to-date within a
   seconds of the query being added to the log. We could probably even shorten
   this to a day or two, because if your node is offline longer than that, you
   probably don't care about your data anyway...
