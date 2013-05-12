go-raft
=======

## Overview

This is an Go implementation of the Raft distributed consensus protocol.
Raft is a protocol by which a cluster of nodes can maintain a replicated state machine.
The state machine is kept in sync through the use of a replicated log.

For more details on Raft, you can read [In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) by Diego Ongaro and John Ousterhout.


## The Raft Protocol

### Overview

Maintaining state in a single process on a single server is easy.
Your process is a single point of authority so there are no conflicts when reading and writing state.
Even multi-threaded processes can rely on locks or coroutines to serialize access to the data.

However, in a distributed system there is no single point of authority.
Servers can crash or the network between two machines can become unavailable or any number of other problems can occur.

A distributed consensus protocol is used for maintaining a consistent state across multiple servers in a cluster.
Many distributed systems are built upon the [Paxos protocol](http://en.wikipedia.org/wiki/Paxos_(computer_science)) but Paxos can be difficult to understand and there are many gaps between Paxos and real world implementation.

An alternative is the [Raft distributed consensus protocol](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) by Diego Ongaro and John Ousterhout is a protocol built with understandability as a primary tenant.
The Raft protocol centers around two main pieces:

1. Leader Election
2. Replicated Log

With these two constructs, you can build a system that can maintain state across multiple servers -- even in the event of multiple failures.


### Leader Election

The Raft protocol effectively works as a master-slave system whereby state changes are written to a single server in the cluster and are distributed out to the rest of the servers in the cluster.
This simplifies the protocol since there can only be one leader at a given time so write conflicts do not occur.



## Project Status

The go-raft library is nearly complete.
There is a reference implementation called [raftd](https://github.com/benbjohnson/raftd) that demonstrates how to use the library 

The library will be considered experimental until it has significant production usage.
I'm writing the library for the purpose of including distributed processing in my behavioral analytics database called [Sky](https://github.com/skydb/sky).
However, I hope other projects can benefit from having a distributed consensus protocol so the go-raft library is available under MIT license.

If you have a project that you're using go-raft in, please add it to this README and send a pull request so others can see implementation examples.
If you have any questions on implementing go-raft in your project, feel free to contact me on [GitHub](https://github.com/benbjohnson), [Twitter](https://twitter.com/benbjohnson) or by e-mail at [ben@skylandlabs.com](mailto:ben@skylandlabs.com).
