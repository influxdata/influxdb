# The Unix Domain Socket Input


## Configuration

Each unix domain socket input allows the binding socket, target database, and target retention policy to be set. If the database does not exist, it will be created automatically when the input is initialized. If the retention policy is not configured, then the default retention policy for the database is used. However if the retention policy is set, the retention policy must be explicitly created. The input will not automatically create it.

Each unix socket input also performs internal batching of the points it receives, as batched writes to the database are more efficient. The default _batch size_ is 1000, _pending batch_ factor is 5, with a _batch timeout_ of 1 second. This means the input will write batches of maximum size 1000, but if a batch has not reached 1000 points within 1 second of the first point being added to a batch, it will emit that batch regardless of size. The pending batch factor controls how many batches can be in memory at once, allowing the input to transmit a batch, while still building other batches.

## Processing

The unix socket input can receive up to 64KB per read, and splits the received data by newline. Each part is then interpreted as line-protocol encoded points, and parsed accordingly.


## Config Examples

One unix socket listener

```
# influxd.conf
...
[[unixsocket]]
  enabled = true
  bind-socket = "/var/run/influxdb.sock" # the bind socket
  database = "telegraf" # Name of the database that will be written to
  batch-size = 5000 # will flush if this many points get buffered
  batch-timeout = "1s" # will flush at least this often even if the batch-size is not reached
  batch-pending = 10 # number of batches that may be pending in memory
  read-buffer = 0 # unix socket read buffer, 0 means to use OS default
...
```

Multiple unix socket listeners

```
# influxd.conf
...
[[unixsocket]]
  # Default unix socket for Telegraf
  enabled = true
  bind-socket = "/var/run/telegraf.sock" # the bind socket
  database = "telegraf" # Name of the database that will be written to
  batch-size = 5000 # will flush if this many points get buffered
  batch-timeout = "1s" # will flush at least this often even if the batch-size is not reached
  batch-pending = 10 # number of batches that may be pending in memory
  read-buffer = 0 # unix socket read buffer size, 0 means to use OS default

[[unixsocket]]
  # High-traffic unix socket
  enabled = true
  bind-socket = "/var/run/mymetrics.sock" # the bind socket
  database = "mymetrics" # Name of the database that will be written to
  batch-size = 5000 # will flush if this many points get buffered
  batch-timeout = "1s" # will flush at least this often even if the batch-size is not reached
  batch-pending = 100 # number of batches that may be pending in memory
  read-buffer = 8388608 # (8*1024*1024) unix socket read buffer size
...
```


