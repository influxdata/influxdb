/*
Package messaging implements a distributed, raft-backed messaging system.

Basics

The broker writes every configuration change and data insert and replicates
those changes to data nodes across the cluster. These changes are segmented into
multiple topics so that they can be parallelized. Configuration changes are
placed in a single "config" topic that is replicated to all data nodes. Each
shard's data is placed in its own topic so that it can be parallized across the
cluster.

*/
package messaging
