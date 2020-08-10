# Cisco model-driven telemetry (MDT)

Cisco model-driven telemetry (MDT) is an input plugin that consumes
telemetry data from Cisco IOS XR, IOS XE and NX-OS platforms. It supports TCP & GRPC dialout transports.
GRPC-based transport can utilize TLS for authentication and encryption.
Telemetry data is expected to be GPB-KV (self-describing-gpb) encoded.

The GRPC dialout transport is supported on various IOS XR (64-bit) 6.1.x and later, IOS XE 16.10 and later, as well as NX-OS 7.x and later platforms.

The TCP dialout transport is supported on IOS XR (32-bit and 64-bit) 6.1.x and later.


### Configuration:

```toml
[[inputs.cisco_telemetry_mdt]]
 ## Telemetry transport can be "tcp" or "grpc".  TLS is only supported when
 ## using the grpc transport.
 transport = "grpc"

 ## Address and port to host telemetry listener
 service_address = ":57000"

 ## Enable TLS; grpc transport only.
 # tls_cert = "/etc/telegraf/cert.pem"
 # tls_key = "/etc/telegraf/key.pem"

 ## Enable TLS client authentication and define allowed CA certificates; grpc
 ##  transport only.
 # tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

 ## Define (for certain nested telemetry measurements with embedded tags) which fields are tags
 # embedded_tags = ["Cisco-IOS-XR-qos-ma-oper:qos/interface-table/interface/input/service-policy-names/service-policy-instance/statistics/class-stats/class-name"]

 ## Define aliases to map telemetry encoding paths to simple measurement names
 [inputs.cisco_telemetry_mdt.aliases]
   ifstats = "ietf-interfaces:interfaces-state/interface/statistics"
```

### Example Output:
```
ifstats,path=ietf-interfaces:interfaces-state/interface/statistics,host=linux,name=GigabitEthernet2,source=csr1kv,subscription=101 in-unicast-pkts=27i,in-multicast-pkts=0i,discontinuity-time="2019-05-23T07:40:23.000362+00:00",in-octets=5233i,in-errors=0i,out-multicast-pkts=0i,out-discards=0i,in-broadcast-pkts=0i,in-discards=0i,in-unknown-protos=0i,out-unicast-pkts=0i,out-broadcast-pkts=0i,out-octets=0i,out-errors=0i 1559150462624000000
ifstats,path=ietf-interfaces:interfaces-state/interface/statistics,host=linux,name=GigabitEthernet1,source=csr1kv,subscription=101 in-octets=3394770806i,in-broadcast-pkts=0i,in-multicast-pkts=0i,out-broadcast-pkts=0i,in-unknown-protos=0i,out-octets=350212i,in-unicast-pkts=9477273i,in-discards=0i,out-unicast-pkts=2726i,out-discards=0i,discontinuity-time="2019-05-23T07:40:23.000363+00:00",in-errors=30i,out-multicast-pkts=0i,out-errors=0i 1559150462624000000
```
