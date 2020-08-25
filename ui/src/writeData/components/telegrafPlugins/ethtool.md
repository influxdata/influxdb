# Ethtool Input Plugin

The ethtool input plugin pulls ethernet device stats.  Fields pulled will depend on the network device and driver

### Configuration:

```toml
# Returns ethtool statistics for given interfaces
[[inputs.ethtool]]
  ## List of interfaces to pull metrics for
  # interface_include = ["eth0"]

  ## List of interfaces to ignore when pulling metrics.
  # interface_exclude = ["eth1"]
```

Interfaces can be included or ignored using

- `interface_include`
- `interface_exclude`

Note that loopback interfaces will be automatically ignored

### Metrics:

Metrics are dependant on the network device and driver

### Example Output:

```
ethtool,driver=igb,host=test01,interface=mgmt0 tx_queue_1_packets=280782i,rx_queue_5_csum_err=0i,tx_queue_4_restart=0i,tx_multicast=7i,tx_queue_1_bytes=39674885i,rx_queue_2_alloc_failed=0i,tx_queue_5_packets=173970i,tx_single_coll_ok=0i,rx_queue_1_drops=0i,tx_queue_2_restart=0i,tx_aborted_errors=0i,rx_queue_6_csum_err=0i,tx_queue_5_restart=0i,tx_queue_4_bytes=64810835i,tx_abort_late_coll=0i,tx_queue_4_packets=109102i,os2bmc_tx_by_bmc=0i,tx_bytes=427527435i,tx_queue_7_packets=66665i,dropped_smbus=0i,rx_queue_0_csum_err=0i,tx_flow_control_xoff=0i,rx_packets=25926536i,rx_queue_7_csum_err=0i,rx_queue_3_bytes=84326060i,rx_multicast=83771i,rx_queue_4_alloc_failed=0i,rx_queue_3_drops=0i,rx_queue_3_csum_err=0i,rx_errors=0i,tx_errors=0i,tx_queue_6_packets=183236i,rx_broadcast=24378893i,rx_queue_7_packets=88680i,tx_dropped=0i,rx_frame_errors=0i,tx_queue_3_packets=161045i,tx_packets=1257017i,rx_queue_1_csum_err=0i,tx_window_errors=0i,tx_dma_out_of_sync=0i,rx_length_errors=0i,rx_queue_5_drops=0i,tx_timeout_count=0i,rx_queue_4_csum_err=0i,rx_flow_control_xon=0i,tx_heartbeat_errors=0i,tx_flow_control_xon=0i,collisions=0i,tx_queue_0_bytes=29465801i,rx_queue_6_drops=0i,rx_queue_0_alloc_failed=0i,tx_queue_1_restart=0i,rx_queue_0_drops=0i,tx_broadcast=9i,tx_carrier_errors=0i,tx_queue_7_bytes=13777515i,tx_queue_7_restart=0i,rx_queue_5_bytes=50732006i,rx_queue_7_bytes=35744457i,tx_deferred_ok=0i,tx_multi_coll_ok=0i,rx_crc_errors=0i,rx_fifo_errors=0i,rx_queue_6_alloc_failed=0i,tx_queue_2_packets=175206i,tx_queue_0_packets=107011i,rx_queue_4_bytes=201364548i,rx_queue_6_packets=372573i,os2bmc_rx_by_host=0i,multicast=83771i,rx_queue_4_drops=0i,rx_queue_5_packets=130535i,rx_queue_6_bytes=139488035i,tx_fifo_errors=0i,tx_queue_5_bytes=84899130i,rx_queue_0_packets=24529563i,rx_queue_3_alloc_failed=0i,rx_queue_7_drops=0i,tx_queue_6_bytes=96288614i,tx_queue_2_bytes=22132949i,tx_tcp_seg_failed=0i,rx_queue_1_bytes=246703840i,rx_queue_0_bytes=1506870738i,tx_queue_0_restart=0i,rx_queue_2_bytes=111344804i,tx_tcp_seg_good=0i,tx_queue_3_restart=0i,rx_no_buffer_count=0i,rx_smbus=0i,rx_queue_1_packets=273865i,rx_over_errors=0i,os2bmc_tx_by_host=0i,rx_queue_1_alloc_failed=0i,rx_queue_7_alloc_failed=0i,rx_short_length_errors=0i,tx_hwtstamp_timeouts=0i,tx_queue_6_restart=0i,rx_queue_2_packets=207136i,tx_queue_3_bytes=70391970i,rx_queue_3_packets=112007i,rx_queue_4_packets=212177i,tx_smbus=0i,rx_long_byte_count=2480280632i,rx_queue_2_csum_err=0i,rx_missed_errors=0i,rx_bytes=2480280632i,rx_queue_5_alloc_failed=0i,rx_queue_2_drops=0i,os2bmc_rx_by_bmc=0i,rx_align_errors=0i,rx_long_length_errors=0i,rx_hwtstamp_cleared=0i,rx_flow_control_xoff=0i 1564658080000000000
ethtool,driver=igb,host=test02,interface=mgmt0 rx_queue_2_bytes=111344804i,tx_queue_3_bytes=70439858i,multicast=83771i,rx_broadcast=24378975i,tx_queue_0_packets=107011i,rx_queue_6_alloc_failed=0i,rx_queue_6_drops=0i,rx_hwtstamp_cleared=0i,tx_window_errors=0i,tx_tcp_seg_good=0i,rx_queue_1_drops=0i,tx_queue_1_restart=0i,rx_queue_7_csum_err=0i,rx_no_buffer_count=0i,tx_queue_1_bytes=39675245i,tx_queue_5_bytes=84899130i,tx_broadcast=9i,rx_queue_1_csum_err=0i,tx_flow_control_xoff=0i,rx_queue_6_csum_err=0i,tx_timeout_count=0i,os2bmc_tx_by_bmc=0i,rx_queue_6_packets=372577i,rx_queue_0_alloc_failed=0i,tx_flow_control_xon=0i,rx_queue_2_drops=0i,tx_queue_2_packets=175206i,rx_queue_3_csum_err=0i,tx_abort_late_coll=0i,tx_queue_5_restart=0i,tx_dropped=0i,rx_queue_2_alloc_failed=0i,tx_multi_coll_ok=0i,rx_queue_1_packets=273865i,rx_flow_control_xon=0i,tx_single_coll_ok=0i,rx_length_errors=0i,rx_queue_7_bytes=35744457i,rx_queue_4_alloc_failed=0i,rx_queue_6_bytes=139488395i,rx_queue_2_csum_err=0i,rx_long_byte_count=2480288216i,rx_queue_1_alloc_failed=0i,tx_queue_0_restart=0i,rx_queue_0_csum_err=0i,tx_queue_2_bytes=22132949i,rx_queue_5_drops=0i,tx_dma_out_of_sync=0i,rx_queue_3_drops=0i,rx_queue_4_packets=212177i,tx_queue_6_restart=0i,rx_packets=25926650i,rx_queue_7_packets=88680i,rx_frame_errors=0i,rx_queue_3_bytes=84326060i,rx_short_length_errors=0i,tx_queue_7_bytes=13777515i,rx_queue_3_alloc_failed=0i,tx_queue_6_packets=183236i,rx_queue_0_drops=0i,rx_multicast=83771i,rx_queue_2_packets=207136i,rx_queue_5_csum_err=0i,rx_queue_5_packets=130535i,rx_queue_7_alloc_failed=0i,tx_smbus=0i,tx_queue_3_packets=161081i,rx_queue_7_drops=0i,tx_queue_2_restart=0i,tx_multicast=7i,tx_fifo_errors=0i,tx_queue_3_restart=0i,rx_long_length_errors=0i,tx_queue_6_bytes=96288614i,tx_queue_1_packets=280786i,tx_tcp_seg_failed=0i,rx_align_errors=0i,tx_errors=0i,rx_crc_errors=0i,rx_queue_0_packets=24529673i,rx_flow_control_xoff=0i,tx_queue_0_bytes=29465801i,rx_over_errors=0i,rx_queue_4_drops=0i,os2bmc_rx_by_bmc=0i,rx_smbus=0i,dropped_smbus=0i,tx_hwtstamp_timeouts=0i,rx_errors=0i,tx_queue_4_packets=109102i,tx_carrier_errors=0i,tx_queue_4_bytes=64810835i,tx_queue_4_restart=0i,rx_queue_4_csum_err=0i,tx_queue_7_packets=66665i,tx_aborted_errors=0i,rx_missed_errors=0i,tx_bytes=427575843i,collisions=0i,rx_queue_1_bytes=246703840i,rx_queue_5_bytes=50732006i,rx_bytes=2480288216i,os2bmc_rx_by_host=0i,rx_queue_5_alloc_failed=0i,rx_queue_3_packets=112007i,tx_deferred_ok=0i,os2bmc_tx_by_host=0i,tx_heartbeat_errors=0i,rx_queue_0_bytes=1506877506i,tx_queue_7_restart=0i,tx_packets=1257057i,rx_queue_4_bytes=201364548i,rx_fifo_errors=0i,tx_queue_5_packets=173970i 1564658090000000000
```
