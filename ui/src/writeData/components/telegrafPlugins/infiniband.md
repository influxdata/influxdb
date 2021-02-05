# InfiniBand Input Plugin

This plugin gathers statistics for all InfiniBand devices and ports on the
system. These are the counters that can be found in
`/sys/class/infiniband/<dev>/port/<port>/counters/`

**Supported Platforms**: Linux

### Configuration

```toml
[[inputs.infiniband]]
  # no configuration
```

### Metrics

Actual metrics depend on the InfiniBand devices, the plugin uses a simple
mapping from counter -> counter value.

[Information about the counters][counters] collected is provided by Mellanox.

[counters]: https://community.mellanox.com/s/article/understanding-mlx5-linux-counters-and-status-parameters

- infiniband
  - tags:
    - device
    - port
  - fields:
    - excessive_buffer_overrun_errors (integer)
    - link_downed (integer)
    - link_error_recovery (integer)
    - local_link_integrity_errors (integer)
    - multicast_rcv_packets (integer)
    - multicast_xmit_packets (integer)
    - port_rcv_constraint_errors (integer)
    - port_rcv_data (integer)
    - port_rcv_errors (integer)
    - port_rcv_packets (integer)
    - port_rcv_remote_physical_errors (integer)
    - port_rcv_switch_relay_errors (integer)
    - port_xmit_constraint_errors (integer)
    - port_xmit_data (integer)
    - port_xmit_discards (integer)
    - port_xmit_packets (integer)
    - port_xmit_wait (integer)
    - symbol_error (integer)
    - unicast_rcv_packets (integer)
    - unicast_xmit_packets (integer)
    - VL15_dropped (integer)



### Example Output

```
infiniband,device=mlx5_0,port=1 VL15_dropped=0i,excessive_buffer_overrun_errors=0i,link_downed=0i,link_error_recovery=0i,local_link_integrity_errors=0i,multicast_rcv_packets=0i,multicast_xmit_packets=0i,port_rcv_constraint_errors=0i,port_rcv_data=237159415345822i,port_rcv_errors=0i,port_rcv_packets=801977655075i,port_rcv_remote_physical_errors=0i,port_rcv_switch_relay_errors=0i,port_xmit_constraint_errors=0i,port_xmit_data=238334949937759i,port_xmit_discards=0i,port_xmit_packets=803162651391i,port_xmit_wait=4294967295i,symbol_error=0i,unicast_rcv_packets=801977655075i,unicast_xmit_packets=803162651391i 1573125558000000000
```
