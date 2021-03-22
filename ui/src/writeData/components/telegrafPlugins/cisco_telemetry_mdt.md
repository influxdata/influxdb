# Cisco model-driven telemetry (MDT) Input Plugin

Cisco model-driven telemetry (MDT) is an input plugin that consumes
telemetry data from Cisco IOS XR, IOS XE and NX-OS platforms. It supports TCP & GRPC dialout transports.
RPC-based transport can utilize TLS for authentication and encryption.
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

 ## Grpc Maximum Message Size, default is 4MB, increase the size.
 max_msg_size = 20000000

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
 [inputs.cisco_telemetry_mdt.dmes]
#    Global Property Xformation.
#    prop1 = "uint64 to int"
#    prop2 = "uint64 to string"
#    prop3 = "string to uint64"
#    prop4 = "string to int64"
#    prop5 = "string to float64"
#    auto-prop-xfrom = "auto-float-xfrom" #Xform any property which is string, and has float number to type float64
#    Per Path property xformation, Name is telemetry configuration under sensor-group, path configuration "WORD         Distinguished Name"
#    Per Path configuration is better as it avoid property collision issue of types.
#    dnpath = '{"Name": "show ip route summary","prop": [{"Key": "routes","Value": "string"}, {"Key": "best-paths","Value": "string"}]}'
#    dnpath2 = '{"Name": "show processes cpu","prop": [{"Key": "kernel_percent","Value": "float"}, {"Key": "idle_percent","Value": "float"}, {"Key": "process","Value": "string"}, {"Key": "user_percent","Value": "float"}, {"Key": "onesec","Value": "float"}]}'
#    dnpath3 = '{"Name": "show processes memory physical","prop": [{"Key": "processname","Value": "string"}]}'
```

### Example Output:
```
ifstats,path=ietf-interfaces:interfaces-state/interface/statistics,host=linux,name=GigabitEthernet2,source=csr1kv,subscription=101 in-unicast-pkts=27i,in-multicast-pkts=0i,discontinuity-time="2019-05-23T07:40:23.000362+00:00",in-octets=5233i,in-errors=0i,out-multicast-pkts=0i,out-discards=0i,in-broadcast-pkts=0i,in-discards=0i,in-unknown-protos=0i,out-unicast-pkts=0i,out-broadcast-pkts=0i,out-octets=0i,out-errors=0i 1559150462624000000
ifstats,path=ietf-interfaces:interfaces-state/interface/statistics,host=linux,name=GigabitEthernet1,source=csr1kv,subscription=101 in-octets=3394770806i,in-broadcast-pkts=0i,in-multicast-pkts=0i,out-broadcast-pkts=0i,in-unknown-protos=0i,out-octets=350212i,in-unicast-pkts=9477273i,in-discards=0i,out-unicast-pkts=2726i,out-discards=0i,discontinuity-time="2019-05-23T07:40:23.000363+00:00",in-errors=30i,out-multicast-pkts=0i,out-errors=0i 1559150462624000000
```

### NX-OS Configuration Example:
```
Requirement      DATA-SOURCE   Configuration
-----------------------------------------
Environment      DME           path sys/ch query-condition query-target=subtree&target-subtree-class=eqptPsuSlot,eqptFtSlot,eqptSupCSlot,eqptPsu,eqptFt,eqptSensor,eqptLCSlot
                 DME           path sys/ch depth 5  (Another configuration option)
Environment      NXAPI         show environment power
                 NXAPI         show environment fan
                 NXAPI         show environment temperature
Interface Stats  DME           path sys/intf query-condition query-target=subtree&target-subtree-class=rmonIfIn,rmonIfOut,rmonIfHCIn,rmonIfHCOut,rmonEtherStats
Interface State  DME           path sys/intf depth unbounded query-condition query-target=subtree&target-subtree-class=l1PhysIf,pcAggrIf,l3EncRtdIf,l3LbRtdIf,ethpmPhysIf
VPC              DME           path sys/vpc query-condition query-target=subtree&target-subtree-class=vpcDom,vpcIf
Resources cpu    DME           path sys/procsys query-condition query-target=subtree&target-subtree-class=procSystem,procSysCore,procSysCpuSummary,procSysCpu,procIdle,procIrq,procKernel,procNice,procSoftirq,procTotal,procUser,procWait,procSysCpuHistory,procSysLoad
Resources Mem    DME           path sys/procsys/sysmem/sysmemused
                               path sys/procsys/sysmem/sysmemusage
                               path sys/procsys/sysmem/sysmemfree
Per Process cpu  DME           path sys/proc depth unbounded query-condition rsp-foreign-subtree=ephemeral
vxlan(svi stats) DME           path sys/bd query-condition query-target=subtree&target-subtree-class=l2VlanStats
BGP              DME           path sys/bgp query-condition query-target=subtree&target-subtree-class=bgpDom,bgpPeer,bgpPeerAf,bgpDomAf,bgpPeerAfEntry,bgpOperRtctrlL3,bgpOperRttP,bgpOperRttEntry,bgpOperAfCtrl
mac dynamic      DME           path sys/mac query-condition query-target=subtree&target-subtree-class=l2MacAddressTable
bfd              DME           path sys/bfd/inst depth unbounded
lldp             DME           path sys/lldp depth unbounded
urib             DME           path sys/urib depth unbounded query-condition rsp-foreign-subtree=ephemeral
u6rib            DME           path sys/u6rib depth unbounded query-condition rsp-foreign-subtree=ephemeral
multicast flow   DME           path sys/mca/show/flows depth unbounded
multicast stats  DME           path sys/mca/show/stats depth unbounded
multicast igmp   NXAPI         show ip igmp groups vrf all
multicast igmp   NXAPI         show ip igmp interface vrf all
multicast igmp   NXAPI         show ip igmp snooping
multicast igmp   NXAPI         show ip igmp snooping groups
multicast igmp   NXAPI         show ip igmp snooping groups detail
multicast igmp   NXAPI         show ip igmp snooping groups summary
multicast igmp   NXAPI         show ip igmp snooping mrouter
multicast igmp   NXAPI         show ip igmp snooping statistics        
multicast pim    NXAPI         show ip pim interface vrf all
multicast pim    NXAPI         show ip pim neighbor vrf all
multicast pim    NXAPI         show ip pim route vrf all
multicast pim    NXAPI         show ip pim rp vrf all
multicast pim    NXAPI         show ip pim statistics vrf all
multicast pim    NXAPI         show ip pim vrf all


```
