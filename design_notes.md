Just some notes about requirements, design, and clustering.

Scalable datastore for metrics, events, and real-time analytics

Requirements
------------

* horizontally scalable
* http interface
* udp interface (low priority)
* persistent
* metadata for time series (low priority)
* perform functions quickly (count, unique, sum, etc.)
* group by time intervals (e.g. count ticks every 5 minutes)
* joining multiple time series to generate new timeseries
* schema-less
* sql-like query language
* support multiple databases with authentication
* single time series should scale horizontally (no hot spots)
* dynamic cluster changes and data balancing
* pubsub layer
* continuous queries (keep connection open and return new points as they arrive)
* Delete ranges of points from any number of timeseries (that should reflect in disk space usage)
* querying should support one or more timeseries (possibly with regex to match on)

New Requirements
----------------
* Easy to backup and restore
* Large time range queries with one column ?
* Optimize for HDD access ?
* What are the common use cases that we should optimize for ?

Modules
-------


           +--------------------+   +--------------------+
           |                    |   |                    |
           |  WebConsole/docs   |   |      Http API      |
           |                    |   |                    |
           +------------------+-+   +-+------------------+
                              |       |
                              |       |
                        +-----+-------+-----------+
                        |                         |
                        |  Lang. Bindings         |
                        |                         |
                        +-----------------+       |
                        |                 |       |
                        |   Query Engine  |       |
                        |                 |       |
                        +-----------------+-------+
                        |                         |
                   +----+ Coordinator (consensus) +-----+
                   |    |                         |     |
                   |    +-------------------------+     |
                   |                                    |
                   |                                    |
          +--------+-----------+                +-------+------------+
          |                    |                |                    |
          |   Storage Engine   |                |   Storage Engine   |
          |                    |                |                    |
          +--------+-----------+                +-------+------------+

Replication & Concensus Notes
-----------------------------

Single raft cluster for which machines are in cluster and who owns which locations.
1. When a write comes into a server, figure out which machine owns the data, proxy out to that.
2. The machine proxies to the server, which assigns a sequence number
3. Each machine in the cluster asks the other machines that own hash ring locations what their latest sequence number is every 10 seconds (this is read repair)

For example, take machines A, B, and C. Say B and C own ring location #2. If a write comes into A it will look up the configuration and pick B or C at random to proxy the write to. Say it goes to B. B assigns a sequence number of 1. It keeps a log for B2 of the writes. It will also keep a log for C2's writes. It then tries to write #1 to C.

If the write is marked as a quorum write, then B won't return a success to A until the data has been written to both B and C. Every so often both B and C will ask each other what their latest writes are.

Taking the example further, if we had server D that also owned ring location 2. B would ask C for writes to C2. If C is down it will ask D for writes to C2. This will ensure that if C fails no data will be lost.

Coding Style
------------

1. Public functions should be at the top of the file, followed by a comment `// private functions` and all private functions.
