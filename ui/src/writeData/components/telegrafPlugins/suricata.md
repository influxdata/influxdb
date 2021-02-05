# Suricata Input Plugin

This plugin reports internal performance counters of the Suricata IDS/IPS
engine, such as captured traffic volume, memory usage, uptime, flow counters,
and much more. It provides a socket for the Suricata log output to write JSON
stats output to, and processes the incoming data to fit Telegraf's format.

### Configuration

```toml
[[inputs.suricata]]
  ## Data sink for Suricata stats log.
  # This is expected to be a filename of a
  # unix socket to be created for listening.
  source = "/var/run/suricata-stats.sock"

  # Delimiter for flattening field keys, e.g. subitem "alert" of "detect"
  # becomes "detect_alert" when delimiter is "_".
  delimiter = "_"
```

### Metrics

Fields in the 'suricata' measurement follow the JSON format used by Suricata's
stats output.
See http://suricata.readthedocs.io/en/latest/performance/statistics.html for
more information.

All fields are numeric.
- suricata
  - tags:
    - thread: `Global` for global statistics (if enabled), thread IDs (e.g. `W#03-enp0s31f6`) for thread-specific statistics
  - fields:
    - app_layer_flow_dcerpc_udp
    - app_layer_flow_dns_tcp
    - app_layer_flow_dns_udp
    - app_layer_flow_enip_udp
    - app_layer_flow_failed_tcp
    - app_layer_flow_failed_udp
    - app_layer_flow_http
    - app_layer_flow_ssh
    - app_layer_flow_tls
    - app_layer_tx_dns_tcp
    - app_layer_tx_dns_udp
    - app_layer_tx_enip_udp
    - app_layer_tx_http
    - app_layer_tx_smtp
    - capture_kernel_drops
    - capture_kernel_packets
    - decoder_avg_pkt_size
    - decoder_bytes
    - decoder_ethernet
    - decoder_gre
    - decoder_icmpv4
    - decoder_icmpv4_ipv4_unknown_ver
    - decoder_icmpv6
    - decoder_invalid
    - decoder_ipv4
    - decoder_ipv6
    - decoder_max_pkt_size
    - decoder_pkts
    - decoder_tcp
    - decoder_tcp_hlen_too_small
    - decoder_tcp_invalid_optlen
    - decoder_teredo
    - decoder_udp
    - decoder_vlan
    - detect_alert
    - dns_memcap_global
    - dns_memuse
    - flow_memuse
    - flow_mgr_closed_pruned
    - flow_mgr_est_pruned
    - flow_mgr_flows_checked
    - flow_mgr_flows_notimeout
    - flow_mgr_flows_removed
    - flow_mgr_flows_timeout
    - flow_mgr_flows_timeout_inuse
    - flow_mgr_new_pruned
    - flow_mgr_rows_checked
    - flow_mgr_rows_empty
    - flow_mgr_rows_maxlen
    - flow_mgr_rows_skipped
    - flow_spare
    - flow_tcp_reuse
    - http_memuse
    - tcp_memuse
    - tcp_pseudo
    - tcp_reassembly_gap
    - tcp_reassembly_memuse
    - tcp_rst
    - tcp_sessions
    - tcp_syn
    - tcp_synack
    - ...


#### Suricata configuration

Suricata needs to deliver the 'stats' event type to a given unix socket for
this plugin to pick up. This can be done, for example, by creating an additional
output in the Suricata configuration file:

```yaml
- eve-log:
    enabled: yes
    filetype: unix_stream
    filename: /tmp/suricata-stats.sock
    types:
      - stats:
         threads: yes
```

#### FreeBSD tuning


Under FreeBSD it is necessary to increase the localhost buffer space to at least 16384, default is 8192 
otherwise messages from Suricata are truncated as they exceed the default available buffer space, 
consequently no statistics are processed by the plugin.

```text
sysctl -w net.local.stream.recvspace=16384
sysctl -w net.local.stream.sendspace=16384
```


### Example Output

```text
suricata,host=myhost,thread=FM#01 flow_mgr_rows_empty=0,flow_mgr_rows_checked=65536,flow_mgr_closed_pruned=0,flow_emerg_mode_over=0,flow_mgr_flows_timeout_inuse=0,flow_mgr_rows_skipped=65535,flow_mgr_bypassed_pruned=0,flow_mgr_flows_removed=0,flow_mgr_est_pruned=0,flow_mgr_flows_notimeout=1,flow_mgr_flows_checked=1,flow_mgr_rows_busy=0,flow_spare=10000,flow_mgr_rows_maxlen=1,flow_mgr_new_pruned=0,flow_emerg_mode_entered=0,flow_tcp_reuse=0,flow_mgr_flows_timeout=0 1568368562545197545
suricata,host=myhost,thread=W#04-wlp4s0 decoder_ltnull_pkt_too_small=0,decoder_ipraw_invalid_ip_version=0,defrag_ipv4_reassembled=0,tcp_no_flow=0,app_layer_flow_tls=1,decoder_udp=25,defrag_ipv6_fragments=0,defrag_ipv4_fragments=0,decoder_tcp=59,decoder_vlan=0,decoder_pkts=84,decoder_vlan_qinq=0,decoder_avg_pkt_size=574,flow_memcap=0,defrag_max_frag_hits=0,tcp_ssn_memcap_drop=0,capture_kernel_packets=84,app_layer_flow_dcerpc_udp=0,app_layer_tx_dns_tcp=0,tcp_rst=0,decoder_icmpv4=0,app_layer_tx_tls=0,decoder_ipv4=84,decoder_erspan=0,decoder_ltnull_unsupported_type=0,decoder_invalid=0,app_layer_flow_ssh=0,capture_kernel_drops=0,app_layer_flow_ftp=0,app_layer_tx_http=0,tcp_pseudo_failed=0,defrag_ipv6_reassembled=0,defrag_ipv6_timeouts=0,tcp_pseudo=0,tcp_sessions=1,decoder_ethernet=84,decoder_raw=0,decoder_sctp=0,app_layer_flow_dns_udp=1,decoder_gre=0,app_layer_flow_http=0,app_layer_flow_imap=0,tcp_segment_memcap_drop=0,detect_alert=0,app_layer_flow_failed_tcp=0,decoder_teredo=0,decoder_mpls=0,decoder_ppp=0,decoder_max_pkt_size=1422,decoder_ipv6=0,tcp_reassembly_gap=0,app_layer_flow_dcerpc_tcp=0,decoder_ipv4_in_ipv6=0,tcp_stream_depth_reached=0,app_layer_flow_dns_tcp=0,app_layer_flow_smtp=0,tcp_syn=1,decoder_sll=0,tcp_invalid_checksum=0,app_layer_tx_dns_udp=1,decoder_bytes=48258,defrag_ipv4_timeouts=0,app_layer_flow_msn=0,decoder_pppoe=0,decoder_null=0,app_layer_flow_failed_udp=3,app_layer_tx_smtp=0,decoder_icmpv6=0,decoder_ipv6_in_ipv6=0,tcp_synack=1,app_layer_flow_smb=0,decoder_dce_pkt_too_small=0 1568368562545174807
suricata,host=myhost,thread=W#01-wlp4s0 tcp_synack=0,app_layer_flow_imap=0,decoder_ipv4_in_ipv6=0,decoder_max_pkt_size=684,decoder_gre=0,defrag_ipv4_timeouts=0,tcp_invalid_checksum=0,decoder_ipv4=53,flow_memcap=0,app_layer_tx_http=0,app_layer_tx_smtp=0,decoder_null=0,tcp_no_flow=0,app_layer_tx_tls=0,app_layer_flow_ssh=0,app_layer_flow_smtp=0,decoder_pppoe=0,decoder_teredo=0,decoder_ipraw_invalid_ip_version=0,decoder_ltnull_pkt_too_small=0,tcp_rst=0,decoder_ppp=0,decoder_ipv6=29,app_layer_flow_dns_udp=3,decoder_vlan=0,app_layer_flow_dcerpc_tcp=0,tcp_syn=0,defrag_ipv4_fragments=0,defrag_ipv6_timeouts=0,decoder_raw=0,defrag_ipv6_reassembled=0,tcp_reassembly_gap=0,tcp_sessions=0,decoder_udp=44,tcp_segment_memcap_drop=0,app_layer_tx_dns_udp=3,app_layer_flow_tls=0,decoder_tcp=37,defrag_ipv4_reassembled=0,app_layer_flow_failed_udp=6,app_layer_flow_ftp=0,decoder_icmpv6=1,tcp_stream_depth_reached=0,capture_kernel_drops=0,decoder_sll=0,decoder_bytes=15883,decoder_ethernet=91,tcp_pseudo=0,app_layer_flow_http=0,decoder_sctp=0,decoder_pkts=91,decoder_avg_pkt_size=174,decoder_erspan=0,app_layer_flow_msn=0,app_layer_flow_smb=0,capture_kernel_packets=91,decoder_icmpv4=0,decoder_ipv6_in_ipv6=0,tcp_ssn_memcap_drop=0,decoder_vlan_qinq=0,decoder_ltnull_unsupported_type=0,decoder_invalid=0,defrag_max_frag_hits=0,tcp_pseudo_failed=0,detect_alert=0,app_layer_tx_dns_tcp=0,app_layer_flow_failed_tcp=0,app_layer_flow_dcerpc_udp=0,app_layer_flow_dns_tcp=0,defrag_ipv6_fragments=0,decoder_mpls=0,decoder_dce_pkt_too_small=0 1568368562545148438
suricata,host=myhost flow_memuse=7094464,tcp_memuse=3276800,tcp_reassembly_memuse=12332832,dns_memuse=0,dns_memcap_state=0,dns_memcap_global=0,http_memuse=0,http_memcap=0 1568368562545144569
suricata,host=myhost,thread=W#07-wlp4s0 app_layer_tx_http=0,app_layer_tx_dns_tcp=0,decoder_vlan=0,decoder_pppoe=0,decoder_sll=0,decoder_tcp=0,flow_memcap=0,app_layer_flow_msn=0,tcp_no_flow=0,tcp_rst=0,tcp_segment_memcap_drop=0,tcp_sessions=0,detect_alert=0,defrag_ipv6_reassembled=0,decoder_ipraw_invalid_ip_version=0,decoder_erspan=0,decoder_icmpv4=0,app_layer_tx_dns_udp=2,decoder_ltnull_pkt_too_small=0,decoder_bytes=1998,decoder_ipv6=1,defrag_ipv4_fragments=0,defrag_ipv6_fragments=0,app_layer_tx_smtp=0,decoder_ltnull_unsupported_type=0,decoder_max_pkt_size=342,app_layer_flow_ftp=0,decoder_ipv6_in_ipv6=0,defrag_ipv4_reassembled=0,defrag_ipv6_timeouts=0,app_layer_flow_dns_tcp=0,decoder_avg_pkt_size=181,defrag_ipv4_timeouts=0,tcp_stream_depth_reached=0,decoder_mpls=0,app_layer_flow_dns_udp=2,tcp_ssn_memcap_drop=0,app_layer_flow_dcerpc_tcp=0,app_layer_flow_failed_udp=2,app_layer_flow_smb=0,app_layer_flow_failed_tcp=0,decoder_invalid=0,decoder_null=0,decoder_gre=0,decoder_ethernet=11,app_layer_flow_ssh=0,defrag_max_frag_hits=0,capture_kernel_drops=0,tcp_pseudo_failed=0,app_layer_flow_smtp=0,decoder_udp=10,decoder_sctp=0,decoder_teredo=0,decoder_icmpv6=1,tcp_pseudo=0,tcp_synack=0,app_layer_tx_tls=0,app_layer_flow_imap=0,capture_kernel_packets=11,decoder_pkts=11,decoder_raw=0,decoder_ppp=0,tcp_syn=0,tcp_invalid_checksum=0,app_layer_flow_tls=0,decoder_ipv4_in_ipv6=0,app_layer_flow_http=0,decoder_dce_pkt_too_small=0,decoder_ipv4=10,decoder_vlan_qinq=0,tcp_reassembly_gap=0,app_layer_flow_dcerpc_udp=0 1568368562545110847
suricata,host=myhost,thread=W#06-wlp4s0 app_layer_tx_smtp=0,decoder_ipv6_in_ipv6=0,decoder_dce_pkt_too_small=0,tcp_segment_memcap_drop=0,tcp_sessions=1,decoder_ppp=0,tcp_pseudo_failed=0,app_layer_tx_dns_tcp=0,decoder_invalid=0,defrag_ipv4_timeouts=0,app_layer_flow_smb=0,app_layer_flow_ssh=0,decoder_bytes=19407,decoder_null=0,app_layer_flow_tls=1,decoder_avg_pkt_size=473,decoder_pkts=41,decoder_pppoe=0,decoder_tcp=32,defrag_ipv4_reassembled=0,tcp_reassembly_gap=0,decoder_raw=0,flow_memcap=0,defrag_ipv6_timeouts=0,app_layer_flow_smtp=0,app_layer_tx_http=0,decoder_sll=0,decoder_udp=8,decoder_ltnull_pkt_too_small=0,decoder_ltnull_unsupported_type=0,decoder_ipv4_in_ipv6=0,decoder_vlan=0,decoder_max_pkt_size=1422,tcp_no_flow=0,app_layer_flow_failed_tcp=0,app_layer_flow_dns_tcp=0,app_layer_flow_ftp=0,decoder_icmpv4=0,defrag_max_frag_hits=0,tcp_rst=0,app_layer_flow_msn=0,app_layer_flow_failed_udp=2,app_layer_flow_dns_udp=0,app_layer_flow_dcerpc_udp=0,decoder_ipv4=39,decoder_ethernet=41,defrag_ipv6_reassembled=0,tcp_ssn_memcap_drop=0,app_layer_tx_tls=0,decoder_gre=0,decoder_vlan_qinq=0,tcp_pseudo=0,app_layer_flow_imap=0,app_layer_flow_dcerpc_tcp=0,defrag_ipv4_fragments=0,defrag_ipv6_fragments=0,tcp_synack=1,app_layer_flow_http=0,app_layer_tx_dns_udp=0,capture_kernel_packets=41,decoder_ipv6=2,tcp_invalid_checksum=0,tcp_stream_depth_reached=0,decoder_ipraw_invalid_ip_version=0,decoder_icmpv6=1,tcp_syn=1,detect_alert=0,capture_kernel_drops=0,decoder_teredo=0,decoder_erspan=0,decoder_sctp=0,decoder_mpls=0 1568368562545084670
suricata,host=myhost,thread=W#02-wlp4s0 decoder_tcp=53,tcp_rst=3,tcp_reassembly_gap=0,defrag_ipv6_timeouts=0,tcp_ssn_memcap_drop=0,app_layer_flow_dcerpc_tcp=0,decoder_max_pkt_size=1422,decoder_ipv6_in_ipv6=0,tcp_no_flow=0,app_layer_flow_ftp=0,app_layer_flow_ssh=0,decoder_pkts=82,decoder_sctp=0,tcp_invalid_checksum=0,app_layer_flow_dns_tcp=0,decoder_ipraw_invalid_ip_version=0,decoder_bytes=26441,decoder_erspan=0,tcp_pseudo_failed=0,tcp_syn=1,app_layer_tx_http=0,app_layer_tx_smtp=0,decoder_teredo=0,decoder_ipv4=80,defrag_ipv4_fragments=0,tcp_stream_depth_reached=0,app_layer_flow_smb=0,capture_kernel_packets=82,decoder_null=0,decoder_ltnull_pkt_too_small=0,decoder_ppp=0,decoder_icmpv6=1,app_layer_flow_dns_udp=2,app_layer_flow_http=0,app_layer_tx_dns_udp=3,decoder_mpls=0,decoder_sll=0,defrag_ipv4_reassembled=0,tcp_segment_memcap_drop=0,app_layer_flow_imap=0,decoder_ltnull_unsupported_type=0,decoder_icmpv4=0,decoder_raw=0,defrag_ipv4_timeouts=0,app_layer_flow_failed_udp=8,decoder_gre=0,capture_kernel_drops=0,defrag_ipv6_reassembled=0,tcp_pseudo=0,app_layer_flow_tls=1,decoder_avg_pkt_size=322,decoder_dce_pkt_too_small=0,decoder_ethernet=82,defrag_ipv6_fragments=0,tcp_sessions=1,tcp_synack=1,app_layer_tx_dns_tcp=0,decoder_vlan=0,flow_memcap=0,decoder_vlan_qinq=0,decoder_udp=28,decoder_invalid=0,detect_alert=0,app_layer_flow_failed_tcp=0,app_layer_tx_tls=0,decoder_pppoe=0,decoder_ipv6=2,decoder_ipv4_in_ipv6=0,defrag_max_frag_hits=0,app_layer_flow_dcerpc_udp=0,app_layer_flow_smtp=0,app_layer_flow_msn=0 1568368562545061864
suricata,host=myhost,thread=W#08-wlp4s0 decoder_dce_pkt_too_small=0,app_layer_tx_dns_tcp=0,decoder_pkts=58,decoder_ppp=0,decoder_raw=0,decoder_ipv4_in_ipv6=0,decoder_max_pkt_size=1392,tcp_invalid_checksum=0,tcp_syn=0,decoder_ipv4=51,decoder_ipv6_in_ipv6=0,decoder_tcp=0,decoder_ltnull_pkt_too_small=0,flow_memcap=0,decoder_udp=58,tcp_ssn_memcap_drop=0,tcp_pseudo=0,app_layer_flow_dcerpc_udp=0,app_layer_flow_dns_udp=5,app_layer_tx_http=0,capture_kernel_drops=0,decoder_vlan=0,tcp_segment_memcap_drop=0,app_layer_flow_ftp=0,app_layer_flow_imap=0,app_layer_flow_http=0,app_layer_flow_tls=0,decoder_icmpv4=0,decoder_sctp=0,defrag_ipv4_timeouts=0,tcp_reassembly_gap=0,detect_alert=0,decoder_ethernet=58,tcp_pseudo_failed=0,decoder_teredo=0,defrag_ipv4_reassembled=0,tcp_sessions=0,app_layer_flow_msn=0,decoder_ipraw_invalid_ip_version=0,tcp_no_flow=0,app_layer_flow_dns_tcp=0,decoder_null=0,defrag_ipv4_fragments=0,app_layer_flow_dcerpc_tcp=0,app_layer_flow_failed_udp=8,app_layer_tx_tls=0,decoder_bytes=15800,decoder_ipv6=7,tcp_stream_depth_reached=0,decoder_invalid=0,decoder_ltnull_unsupported_type=0,app_layer_tx_dns_udp=6,decoder_pppoe=0,decoder_avg_pkt_size=272,decoder_erspan=0,defrag_ipv6_timeouts=0,app_layer_flow_failed_tcp=0,decoder_gre=0,decoder_sll=0,defrag_max_frag_hits=0,app_layer_flow_ssh=0,capture_kernel_packets=58,decoder_mpls=0,decoder_vlan_qinq=0,tcp_rst=0,app_layer_flow_smb=0,app_layer_tx_smtp=0,decoder_icmpv6=0,defrag_ipv6_fragments=0,defrag_ipv6_reassembled=0,tcp_synack=0,app_layer_flow_smtp=0 1568368562545035575
suricata,host=myhost,thread=W#05-wlp4s0 tcp_reassembly_gap=0,capture_kernel_drops=0,decoder_ltnull_unsupported_type=0,tcp_sessions=0,tcp_stream_depth_reached=0,tcp_pseudo_failed=0,app_layer_flow_failed_tcp=0,app_layer_tx_dns_tcp=0,decoder_null=0,decoder_dce_pkt_too_small=0,decoder_udp=7,tcp_rst=3,app_layer_flow_dns_tcp=0,decoder_invalid=0,defrag_ipv4_reassembled=0,tcp_synack=0,app_layer_flow_ftp=0,decoder_bytes=3117,decoder_pppoe=0,app_layer_flow_dcerpc_tcp=0,app_layer_flow_smb=0,decoder_ipv6_in_ipv6=0,decoder_ipraw_invalid_ip_version=0,app_layer_flow_imap=0,app_layer_tx_dns_udp=2,decoder_ppp=0,decoder_ipv4=21,decoder_tcp=14,flow_memcap=0,tcp_syn=0,tcp_invalid_checksum=0,decoder_teredo=0,decoder_ltnull_pkt_too_small=0,defrag_max_frag_hits=0,app_layer_tx_tls=0,decoder_pkts=24,decoder_sll=0,defrag_ipv6_fragments=0,app_layer_flow_dcerpc_udp=0,app_layer_flow_smtp=0,decoder_icmpv6=3,defrag_ipv6_timeouts=0,decoder_ipv6=3,decoder_raw=0,defrag_ipv6_reassembled=0,tcp_no_flow=0,detect_alert=0,app_layer_flow_tls=0,decoder_ethernet=24,decoder_vlan=0,decoder_icmpv4=0,decoder_ipv4_in_ipv6=0,app_layer_flow_failed_udp=1,decoder_mpls=0,decoder_max_pkt_size=653,decoder_sctp=0,defrag_ipv4_timeouts=0,tcp_ssn_memcap_drop=0,app_layer_flow_dns_udp=1,app_layer_tx_smtp=0,capture_kernel_packets=24,decoder_vlan_qinq=0,decoder_gre=0,app_layer_flow_ssh=0,app_layer_flow_msn=0,defrag_ipv4_fragments=0,app_layer_flow_http=0,tcp_segment_memcap_drop=0,tcp_pseudo=0,app_layer_tx_http=0,decoder_erspan=0,decoder_avg_pkt_size=129 1568368562545009684
suricata,host=myhost,thread=W#03-wlp4s0 app_layer_flow_failed_tcp=0,decoder_teredo=0,decoder_ipv6_in_ipv6=0,tcp_pseudo_failed=0,tcp_stream_depth_reached=0,tcp_syn=0,decoder_gre=0,tcp_segment_memcap_drop=0,tcp_ssn_memcap_drop=0,app_layer_tx_smtp=0,decoder_raw=0,decoder_ltnull_pkt_too_small=0,tcp_sessions=0,tcp_reassembly_gap=0,app_layer_flow_ssh=0,app_layer_flow_imap=0,decoder_ipv4=463,decoder_ethernet=463,capture_kernel_packets=463,decoder_pppoe=0,defrag_ipv4_reassembled=0,app_layer_flow_tls=0,app_layer_flow_dcerpc_udp=0,app_layer_flow_dns_udp=0,decoder_vlan=0,decoder_ipraw_invalid_ip_version=0,decoder_mpls=0,tcp_no_flow=0,decoder_avg_pkt_size=445,decoder_udp=432,flow_memcap=0,app_layer_tx_dns_udp=0,app_layer_flow_msn=0,app_layer_flow_http=0,app_layer_flow_dcerpc_tcp=0,decoder_ipv6=0,decoder_ipv4_in_ipv6=0,defrag_ipv4_timeouts=0,defrag_ipv4_fragments=0,defrag_ipv6_timeouts=0,decoder_sctp=0,defrag_ipv6_fragments=0,app_layer_flow_dns_tcp=0,app_layer_tx_tls=0,defrag_max_frag_hits=0,decoder_bytes=206345,decoder_vlan_qinq=0,decoder_invalid=0,decoder_ppp=0,tcp_rst=0,detect_alert=0,capture_kernel_drops=0,app_layer_flow_failed_udp=4,decoder_null=0,decoder_icmpv4=0,decoder_icmpv6=0,decoder_ltnull_unsupported_type=0,defrag_ipv6_reassembled=0,tcp_invalid_checksum=0,tcp_synack=0,decoder_tcp=31,tcp_pseudo=0,app_layer_flow_smb=0,app_layer_flow_smtp=0,decoder_max_pkt_size=1463,decoder_dce_pkt_too_small=0,app_layer_tx_http=0,decoder_pkts=463,decoder_sll=0,app_layer_flow_ftp=0,app_layer_tx_dns_tcp=0,decoder_erspan=0 1568368562544966078
```
