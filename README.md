chronosdb
=========

Scalable datastore for metrics, events, and real-time analytics

Requirements
------------

* horizontal scalable
* http interface
* udp interface (low priority)
* persistent
* metadata for time series
* perform functions quickly (count, unique, sum, etc.)
* group by time intervals (e.g. count ticks every 5 minutes)
* joining multiple time series to generate new timeseries
* dynamic schema
* filter/query language (sql subset) with where clauses
* support multiple databases with read/write api key
* single time series should scale horizontally (no hot spots)
* dynamic cluster changes and data balancing
* pubsub layer
* continuous queries (keep connection open and return new points as they arrive)
* Delete ranges of points from any number of timeseries (that should reflect in disk space usage)
* querying should support one or more timeseries (possibly with regex to match on)

Modules
-------


       +--------------------+   +--------------------+  +--------------------+
       |                    |   |                    |  |                    |
       |  WebConsole/docs   |   |      Http API      |  |  Lang. Bindings    |
       |                    |   |                    |  |                    |
       +------------------+-+   +-+------------------+  +----+---------------+
                          |       |                          |
                          |       |                          |
                        +-+-------+-------+                  |
                        |                 +------------------+
                        |   Query Engine  |
                        |                 |
                        +-----------------+---+
                        |                     |
                        |  Processing Engine  |
                        |                     |
                        +---------------------+
                        |                     |
                   +----+     Coordinator     +---------+
                   |    |                     |         |
                   |    +---------------------+         |
                   |                                    |
                   |                                    |
          +--------+-----------+                +-------+------------+
          |                    |                |                    |
          |   Storage Engine   |                |   Storage Engine   |
          |                    |                |                    |
          +--------+-----------+                +-------+------------+
                   |                                    |
                   |                                    |
                   |    +---------------------+         |
                   |    |                     |         |
                   +----+  Consensus Engine   +---------+
                        |                     |
                        +---------------------+

