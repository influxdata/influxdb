# Couchbase Input Plugin

## Configuration:

```toml
# Read per-node and per-bucket metrics from Couchbase
[[inputs.couchbase]]
  ## specify servers via a url matching:
  ##  [protocol://][:password]@address[:port]
  ##  e.g.
  ##    http://couchbase-0.example.com/
  ##    http://admin:secret@couchbase-0.example.com:8091/
  ##
  ## If no servers are specified, then localhost is used as the host.
  ## If no protocol is specified, HTTP is used.
  ## If no port is specified, 8091 is used.
  servers = ["http://localhost:8091"]
```

## Measurements:

### couchbase_node

Tags:
- cluster: sanitized string from `servers` configuration field e.g.: `http://user:password@couchbase-0.example.com:8091/endpoint` -> `http://couchbase-0.example.com:8091/endpoint`
- hostname: Couchbase's name for the node and port, e.g., `172.16.10.187:8091`

Fields:
- memory_free (unit: bytes, example: 23181365248.0)
- memory_total (unit: bytes, example: 64424656896.0)

### couchbase_bucket

Tags:
- cluster: whatever you called it in `servers` in the configuration, e.g.: `http://couchbase-0.example.com/`)
- bucket: the name of the couchbase bucket, e.g., `blastro-df`

Fields:
- quota_percent_used (unit: percent, example: 68.85424936294555)
- ops_per_sec (unit: count, example: 5686.789686789687)
- disk_fetches (unit: count, example: 0.0)
- item_count (unit: count, example: 943239752.0)
- disk_used (unit: bytes, example: 409178772321.0)
- data_used (unit: bytes, example: 212179309111.0)
- mem_used (unit: bytes, example: 202156957464.0)


## Example output

```
couchbase_node,cluster=http://localhost:8091/,hostname=172.17.0.2:8091 memory_free=7705575424,memory_total=16558182400 1547829754000000000
couchbase_bucket,bucket=beer-sample,cluster=http://localhost:8091/ quota_percent_used=27.09285736083984,ops_per_sec=0,disk_fetches=0,item_count=7303,disk_used=21662946,data_used=9325087,mem_used=28408920 1547829754000000000
```
