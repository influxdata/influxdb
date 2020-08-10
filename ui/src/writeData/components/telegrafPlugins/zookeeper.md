## Zookeeper Input Plugin

The zookeeper plugin collects variables outputted from the 'mntr' command
[Zookeeper Admin](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html).

### Configuration

```toml
# Reads 'mntr' stats from one or many zookeeper servers
[[inputs.zookeeper]]
  ## An array of address to gather stats about. Specify an ip or hostname
  ## with port. ie localhost:2181, 10.0.0.1:2181, etc.

  ## If no servers are specified, then localhost is used as the host.
  ## If no port is specified, 2181 is used
  servers = [":2181"]

  ## Timeout for metric collections from all servers.  Minimum timeout is "1s".
  # timeout = "5s"

  ## Optional TLS Config
  # enable_tls = true
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## If false, skip chain & host verification
  # insecure_skip_verify = true
```

### Metrics:

Exact field names are based on Zookeeper response and may vary between
configuration, platform, and version.

- zookeeper
  - tags:
    - server
    - port
    - state
  - fields:
    - approximate_data_size (integer)
    - avg_latency (integer)
    - ephemerals_count (integer)
    - max_file_descriptor_count (integer)
    - max_latency (integer)
    - min_latency (integer)
    - num_alive_connections (integer)
    - open_file_descriptor_count (integer)
    - outstanding_requests (integer)
    - packets_received (integer)
    - packets_sent (integer)
    - version (string)
    - watch_count (integer)
    - znode_count (integer)
    - followers (integer, leader only)
    - synced_followers (integer, leader only)
    - pending_syncs (integer, leader only)

### Debugging:

If you have any issues please check the direct Zookeeper output using netcat:
```sh
$ echo mntr | nc localhost 2181
zk_version      3.4.9-3--1, built on Thu, 01 Jun 2017 16:26:44 -0700
zk_avg_latency  0
zk_max_latency  0
zk_min_latency  0
zk_packets_received     8
zk_packets_sent 7
zk_num_alive_connections        1
zk_outstanding_requests 0
zk_server_state standalone
zk_znode_count  129
zk_watch_count  0
zk_ephemerals_count     0
zk_approximate_data_size        10044
zk_open_file_descriptor_count   44
zk_max_file_descriptor_count    4096
```

### Example Output

```
zookeeper,server=localhost,port=2181,state=standalone ephemerals_count=0i,approximate_data_size=10044i,open_file_descriptor_count=44i,max_latency=0i,packets_received=7i,outstanding_requests=0i,znode_count=129i,max_file_descriptor_count=4096i,version="3.4.9-3--1",avg_latency=0i,packets_sent=6i,num_alive_connections=1i,watch_count=0i,min_latency=0i 1522351112000000000
```
