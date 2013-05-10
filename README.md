go-raft
=======

## Overview

This is an Go implementation of the Raft distributed consensus protocol.
Raft is a protocol by which a cluster of nodes can maintain a replicated state machine.
The state machine is kept in sync through the use of a replicated log.

For more details on Raft, you can read [In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) by Diego Ongaro and John Ousterhout.


## Project Status

The go-raft library is nearly complete.
There is a reference implementation called [raftd](https://github.com/benbjohnson/raftd) that demonstrates how to use the library 

The library will be considered experimental until it has significant production usage.
I'm writing the library for the purpose of including distributed processing in my behavioral analytics database called [Sky](https://github.com/skydb/sky).
However, I hope other projects can benefit from having a distributed consensus protocol so the go-raft library is available under MIT license.

If you have a project that you're using go-raft in, please add it to this README and send a pull request so others can see implementation examples.
If you have any questions on implementing go-raft in your project, feel free to contact me on [GitHub](https://github.com/benbjohnson), [Twitter](https://twitter.com/benbjohnson) or by e-mail at [ben@skylandlabs.com](mailto:ben@skylandlabs.com).
