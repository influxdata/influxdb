/*
Package raft implements a streaming version of the Raft protocol.

The Raft distributed consensus protocol is used for maintaining consistency
in a distributed system. It implements a replicated log between multiple nodes.
This log is a series of entries that represent commands to be executed on the
node's state. By executing the same commands on every node in the same order,
the state of each node is consistent.

For a more detailed understanding of the Raft protocol, please see the original
Raft paper by Diego Ongaro:

https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf


Streaming Raft

The standard implementation of Raft implements a two RPC calls between the
leader and the followers: AppendEntries & RequestVote. The RequestVote is the
same as in the standard implementation. The AppendEntries RPC is split into
two calls: a streaming log and a heartbeat.

One issue with the original Raft implementation is that AppendEntries calls
were required to wait between calls. This would cause a delay of a few
milliseconds but would ultimitely limit the throughput that Raft could acheive.
This can be mitigated by pipelining requests so they don't have to wait for one
another. Pipelining helps significantly but there is still overhead in tracking
each request and there's a limit on outstanding requests.

Another issue with AppendEntries is that it combines the log replication with
the heartbeat mechanism. This is beneficial in that it causes log replication
issues such as disk write errors to cause heartbeat errors but it also has the
problem that replicating too many log entries will cause a heartbeat to be
missed.

Streaming Raft tries to mitigate by separating log replication from heartbeats.
When a node becomes a leader, it begins sending heartbeats to establish
dominance. When a follower node recognizes a new leader, it connects to the
leader using a long-running connection that simply streams the log from where
the follower left off. The handshake process required by the standard Raft
implementation on every AppendEntries request is now handled once upon
connection.


*/
package raft
