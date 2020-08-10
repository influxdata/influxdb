# RethinkDB Input

Collect metrics from [RethinkDB](https://www.rethinkdb.com/).

### Configuration

This section contains the default TOML to configure the plugin.  You can
generate it using `telegraf --usage rethinkdb`.

```toml
[[inputs.rethinkdb]]
  ## An array of URI to gather stats about. Specify an ip or hostname
  ## with optional port add password. ie,
  ##   rethinkdb://user:auth_key@10.10.3.30:28105,
  ##   rethinkdb://10.10.3.33:18832,
  ##   10.0.0.1:10000, etc.
  servers = ["127.0.0.1:28015"]

  ## If you use actual rethinkdb of > 2.3.0 with username/password authorization,
  ## protocol have to be named "rethinkdb2" - it will use 1_0 H.
  # servers = ["rethinkdb2://username:password@127.0.0.1:28015"]

  ## If you use older versions of rethinkdb (<2.2) with auth_key, protocol
  ## have to be named "rethinkdb".
  # servers = ["rethinkdb://username:auth_key@127.0.0.1:28015"]
```

### Metrics

- rethinkdb
  - tags:
    - type
    - ns
    - rethinkdb_host
    - rethinkdb_hostname
  - fields:
    - cache_bytes_in_use (integer, bytes)
    - disk_read_bytes_per_sec (integer, reads)
    - disk_read_bytes_total (integer, bytes)
    - disk_written_bytes_per_sec (integer, bytes)
    - disk_written_bytes_total (integer, bytes)
    - disk_usage_data_bytes (integer, bytes)
    - disk_usage_garbage_bytes (integer, bytes)
    - disk_usage_metadata_bytes (integer, bytes)
    - disk_usage_preallocated_bytes (integer, bytes)

+ rethinkdb_engine
  - tags:
    - type
    - ns
    - rethinkdb_host
    - rethinkdb_hostname
  - fields:
    - active_clients (integer, clients)
    - clients (integer, clients)
    - queries_per_sec (integer, queries)
    - total_queries (integer, queries)
    - read_docs_per_sec (integer, reads)
    - total_reads (integer, reads)
    - written_docs_per_sec (integer, writes)
    - total_writes (integer, writes)
