[03/06/14 16:51:05] [INFO] Redirectoring logging to influxdb.log
[03/06/14 16:51:05] [INFO] Starting Influx Server bound to 192.168.0.18 ...
[03/06/14 16:51:05] [INFO] 
+---------------------------------------------+
|  _____        __ _            _____  ____   |
| |_   _|      / _| |          |  __ \|  _ \  |
|   | |  _ __ | |_| |_   ___  _| |  | | |_) | |
|   | | | '_ \|  _| | | | \ \/ / |  | |  _ <  |
|  _| |_| | | | | | | |_| |>  <| |__| | |_) | |
| |_____|_| |_|_| |_|\__,_/_/\_\_____/|____/  |
+---------------------------------------------+


[03/06/14 16:51:05] [INFO] Opening database at /tmp/influxdb/development/db
[03/06/14 16:51:05] [DEBG] Recovering from previous state from file offset: 1306322
[03/06/14 16:51:05] [INFO] Ssl will be disabled since the ssl port or certificate path weren't set
[03/06/14 16:51:05] [INFO] Initializing Raft HTTP server
[03/06/14 16:51:05] [INFO] Raft Server Listening at http://ubuntu:8090
[03/06/14 16:51:05] [INFO] ProtobufServer listening on 192.168.0.18:8099
[03/06/14 16:51:05] [INFO] Initializing Raft Server: /tmp/influxdb/development/raft 8090
[03/06/14 16:51:05] [INFO] Recovered from log
[03/06/14 16:51:05] [INFO] Waiting for local server to be added
[03/06/14 16:51:05] [INFO] (raft:9b3ec29) Selected as leader. Starting leader loop.
[03/06/14 16:51:05] [INFO] Added server to cluster config: 1, http://ubuntu:8090, ubuntu:8099
[03/06/14 16:51:05] [INFO] Checking whether this is the local server new: ubuntu:8099, local: ubuntu:8099

[03/06/14 16:51:05] [INFO] Added the local server
[03/06/14 16:51:05] [INFO] DATASTORE: opening or creating shard /tmp/influxdb/development/db/shard_db/00001
[03/06/14 16:51:05] [INFO] Recovering from log...
[03/06/14 16:51:05] [INFO] Initializing write buffer with buffer size of 10000
[03/06/14 16:51:05] [INFO] Replaying from /tmp/influxdb/development/wal/log.2
[03/06/14 16:51:05] [DEBG] Replaying from file offset 1016000
[03/06/14 16:51:05] [INFO] Adding short term shard: 1 - start: Wed Mar 5 16:00:00 -0800 PST 2014. end: Wed Mar 12 17:00:00 -0700 PDT 2014. isLocal: %!d(bool=true). servers: [%!s(uint32=1)]
[03/06/14 16:51:05] [INFO] /tmp/influxdb/development/wal/log.2 yielded 2 requests
[03/06/14 16:51:05] [INFO] recovered
[03/06/14 16:51:05] [INFO] Connecting to other nodes in the cluster
[03/06/14 16:51:05] [INFO] Starting admin interface on port 8083
[03/06/14 16:51:05] [INFO] Starting Http Api server on port 8086
