/*
Package multiraft implements a two-tier Raft implementation. It is used for
distributing sharded logs across a cluster efficiently.


Basics

The original Raft paper describes how to safely replicate a log of changes for
a single cluster. This provides safety (and read scaling if stale reads are
allowed) but does not improve write scaling because all writes must go through
a single master.

With multi-raft, there are two separate tiers of Raft logs. The top tier manages
the cluster-wide membership and any global state. The second tier manages any
shard-level data. In many sharded applications, global state changes are
relatively rare so most data can be managed and replicated through the
shard-level tier.


Replicating cluster configuration

To provide per-shard consistency, the shard's leader reads from the
cluster-level committed log and applies all cluster commands to the shard.
This ensures that shards have a single log to apply and it removes
nondeterminism at the shard level.


See Also

https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

*/
package multiraft
