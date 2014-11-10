package collectd

import (
	"encoding/hex"
	"testing"

	"github.com/kimor79/gollectd"
	. "launchpad.net/gocheck"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type CollectdApiSuite struct {
	server *Server
}

var _ = Suite(&CollectdApiSuite{})

func (cas *CollectdApiSuite) SetUpSuite(c *C) {
}

func (cas *CollectdApiSuite) TestPacketToSeriesWithUnixTimestamp(c *C) {
	// Raw data from a Wireshark capture
	buf, err := hex.DecodeString("000000167066312d36322d3231302d39342d313733000001000c00000000544928ff0007000c00000000000000050002000c656e74726f7079000004000c656e74726f7079000006000f0001010000000000007240000200086370750000030006310000040008637075000005000969646c65000006000f0001000000000000a674620005000977616974000006000f0001000000000000000000000200076466000003000500000400076466000005000d6c6976652d636f7700000600180002010100000000a090b641000000a0cb6a2742000200086370750000030006310000040008637075000005000e696e74657272757074000006000f00010000000000000000fe0005000c736f6674697271000006000f000100000000000000000000020007646600000300050000040007646600000500096c6976650000060018000201010000000000000000000000e0ec972742000200086370750000030006310000040008637075000005000a737465616c000006000f00010000000000000000000003000632000005000975736572000006000f0001000000000000005f36000500096e696365000006000f0001000000000000000ad80002000e696e746572666163650000030005000004000e69665f6f6374657473000005000b64756d6d79300000060018000200000000000000000000000000000000041a000200076466000004000764660000050008746d70000006001800020101000000000000f240000000a0ea972742000200086370750000030006320000040008637075000005000b73797374656d000006000f00010000000000000045d30002000e696e746572666163650000030005000004000f69665f7061636b657473000005000b64756d6d79300000060018000200000000000000000000000000000000000f000200086370750000030006320000040008637075000005000969646c65000006000f0001000000000000a66480000200076466000003000500000400076466000005000d72756e2d6c6f636b000006001800020101000000000000000000000000000054410002000e696e74657266616365000004000e69665f6572726f7273000005000b64756d6d793000000600180002000000000000000000000000000000000000000200086370750000030006320000040008637075000005000977616974000006000f00010000000000000000000005000e696e74657272757074000006000f0001000000000000000132")
	c.Assert(err, IsNil)

	// Get a collectd types DB
	typesDB, err := gollectd.TypesDB([]byte(typesDBText))
	c.Assert(err, IsNil)

	// Use gollectd to parse raw packet data into *[]gollectd.Packet
	packets, err := gollectd.Packets(buf, typesDB)
	c.Assert(err, IsNil)
	c.Assert(len(*packets), Equals, 19)

	// Test InfluxDB collectd API's packetToSeries function
	packet := &(*packets)[0]
	series := packetToSeries(packet)
	timestamp := *series[0].Points[0].Timestamp
	values := series[0].Points[0].Values
	dsname := values[5].GetStringValue()
	dsval := values[7].GetDoubleValue()
	c.Assert(timestamp, Equals, int64(1414080767000000))
	c.Assert(dsname, Equals, "value")
	c.Assert(dsval, Equals, float64(288))
}

func (cas *CollectdApiSuite) TestPacketToSeriesWithHiResTimestamp(c *C) {
	// Raw data from a Wireshark capture
	buf, err := hex.DecodeString("0000000e6c6f63616c686f7374000008000c1512b2e40f5da16f0009000c00000002800000000002000e70726f636573736573000004000d70735f7374617465000005000c72756e6e696e67000006000f000101000000000000f03f0008000c1512b2e40f5db90f0005000d736c656570696e67000006000f0001010000000000c06f400008000c1512b2e40f5dc4a40005000c7a6f6d62696573000006000f00010100000000000000000008000c1512b2e40f5de10b0005000c73746f70706564000006000f00010100000000000000000008000c1512b2e40f5deac20005000b706167696e67000006000f00010100000000000000000008000c1512b2e40f5df59b0005000c626c6f636b6564000006000f00010100000000000000000008000c1512b2e40f7ee0610004000e666f726b5f726174650000050005000006000f000102000000000004572f0008000c1512b2e68e0635e6000200086370750000030006300000040008637075000005000975736572000006000f0001020000000000204f9c0008000c1512b2e68e0665d6000500096e696365000006000f000102000000000000caa30008000c1512b2e68e06789c0005000b73797374656d000006000f00010200000000000607050008000c1512b2e68e06818e0005000969646c65000006000f0001020000000003b090ae0008000c1512b2e68e068bcf0005000977616974000006000f000102000000000000f6810008000c1512b2e68e069c7d0005000e696e74657272757074000006000f000102000000000000001d0008000c1512b2e68e069fec0005000c736f6674697271000006000f0001020000000000000a2a0008000c1512b2e68e06a2b20005000a737465616c000006000f00010200000000000000000008000c1512b2e68e0708d60003000631000005000975736572000006000f00010200000000001d48c60008000c1512b2e68e070c16000500096e696365000006000f0001020000000000007fe60008000c1512b2e68e0710790005000b73797374656d000006000f00010200000000000667890008000c1512b2e68e0713bb0005000969646c65000006000f00010200000000025d0e470008000c1512b2e68e0717790005000977616974000006000f000102000000000002500e0008000c1512b2e68e071bc00005000e696e74657272757074000006000f00010200000000000000000008000c1512b2e68e071f800005000c736f6674697271000006000f00010200000000000006050008000c1512b2e68e07221e0005000a737465616c000006000f00010200000000000000000008000c1512b2e68e0726eb0003000632000005000975736572000006000f00010200000000001ff3e40008000c1512b2e68e0728cb000500096e696365000006000f000102000000000000ca210008000c1512b2e68e072ae70005000b73797374656d000006000f000102000000000006eabe0008000c1512b2e68e072f2f0005000977616974000006000f000102000000000000c1300008000c1512b2e68e072ccb0005000969646c65000006000f00010200000000025b5abb0008000c1512b2e68e07312c0005000e696e74657272757074000006000f00010200000000000000070008000c1512b2e68e0733520005000c736f6674697271000006000f00010200000000000007260008000c1512b2e68e0735b60005000a737465616c000006000f00010200000000000000000008000c1512b2e68e07828d0003000633000005000975736572000006000f000102000000000020f50a0008000c1512b2e68e0787ac000500096e696365000006000f0001020000000000008368")
	c.Assert(err, IsNil)

	// Get a collectd types DB
	typesDB, err := gollectd.TypesDB([]byte(typesDBText))
	c.Assert(err, IsNil)

	// Use gollectd to parse raw packet data into *[]gollectd.Packet
	packets, err := gollectd.Packets(buf, typesDB)
	c.Assert(err, IsNil)
	c.Assert(len(*packets), Equals, 33)

	// Test InfluxDB collectd API's packetToSeries function
	packet := &(*packets)[0]
	series := packetToSeries(packet)
	timestamp := *series[0].Points[0].Timestamp
	values := series[0].Points[0].Values
	dsname := values[5].GetStringValue()
	dsval := values[7].GetDoubleValue()
	c.Assert(timestamp, Equals, int64(1414187920000000))
	c.Assert(dsname, Equals, "value")
	c.Assert(dsval, Equals, float64(1))
}

func (cas *CollectdApiSuite) TestPacketToSeriesWithMultiDataSet(c *C) {
	// Raw data from a Wireshark capture
	buf, err := hex.DecodeString("0000001a7377697463682d3137322e31362e3233312e323530000008000c15160fd86fdb575500020009736e6d70000004000e69665f6f637465747300000500184769676162697445746865726e6574305f3138000006001800020202000000000fae0fb7000000000e7d8ac3")
	c.Assert(err, IsNil)

	// Get a collectd types DB
	typesDB, err := gollectd.TypesDB([]byte(typesDBText))
	c.Assert(err, IsNil)

	// Use gollectd to parse raw packet data into *[]gollectd.Packet
	packets, err := gollectd.Packets(buf, typesDB)
	c.Assert(err, IsNil)
	c.Assert(len(*packets), Equals, 1)

	// Test InfluxDB collectd API's packetToSeries function
	packet := &(*packets)[0]
	series := packetToSeries(packet)

	timestamp0 := *series[0].Points[0].Timestamp
	values0 := series[0].Points[0].Values
	dsname0 := values0[5].GetStringValue()
	dsval0 := values0[7].GetDoubleValue()
	c.Assert(timestamp0, Equals, int64(1415069537000000))
	c.Assert(dsname0, Equals, "rx")
	c.Assert(dsval0, Equals, float64(263065527))

	timestamp1 := *series[1].Points[0].Timestamp
	values1 := series[1].Points[0].Values
	dsname1 := values1[5].GetStringValue()
	dsval1 := values1[7].GetDoubleValue()
	c.Assert(timestamp1, Equals, int64(1415069537000000))
	c.Assert(dsname1, Equals, "tx")
	c.Assert(dsval1, Equals, float64(243108547))
}

// Taken from /usr/share/collectd/types.db on a Ubuntu system
var typesDBText = `
absolute		value:ABSOLUTE:0:U
apache_bytes		value:DERIVE:0:U
apache_connections	value:GAUGE:0:65535
apache_idle_workers	value:GAUGE:0:65535
apache_requests		value:DERIVE:0:U
apache_scoreboard	value:GAUGE:0:65535
ath_nodes		value:GAUGE:0:65535
ath_stat		value:DERIVE:0:U
backends		value:GAUGE:0:65535
bitrate			value:GAUGE:0:4294967295
bytes			value:GAUGE:0:U
cache_eviction		value:DERIVE:0:U
cache_operation		value:DERIVE:0:U
cache_ratio		value:GAUGE:0:100
cache_result		value:DERIVE:0:U
cache_size		value:GAUGE:0:4294967295
charge			value:GAUGE:0:U
compression_ratio	value:GAUGE:0:2
compression		uncompressed:DERIVE:0:U, compressed:DERIVE:0:U
connections		value:DERIVE:0:U
conntrack		value:GAUGE:0:4294967295
contextswitch		value:DERIVE:0:U
counter			value:COUNTER:U:U
cpufreq			value:GAUGE:0:U
cpu			value:DERIVE:0:U
current_connections	value:GAUGE:0:U
current_sessions	value:GAUGE:0:U
current			value:GAUGE:U:U
delay			value:GAUGE:-1000000:1000000
derive			value:DERIVE:0:U
df_complex		value:GAUGE:0:U
df_inodes		value:GAUGE:0:U
df			used:GAUGE:0:1125899906842623, free:GAUGE:0:1125899906842623
disk_latency		read:GAUGE:0:U, write:GAUGE:0:U
disk_merged		read:DERIVE:0:U, write:DERIVE:0:U
disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
disk_ops_complex	value:DERIVE:0:U
disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
disk_time		read:DERIVE:0:U, write:DERIVE:0:U
dns_answer		value:DERIVE:0:U
dns_notify		value:DERIVE:0:U
dns_octets		queries:DERIVE:0:U, responses:DERIVE:0:U
dns_opcode		value:DERIVE:0:U
dns_qtype_cached	value:GAUGE:0:4294967295
dns_qtype		value:DERIVE:0:U
dns_query		value:DERIVE:0:U
dns_question		value:DERIVE:0:U
dns_rcode		value:DERIVE:0:U
dns_reject		value:DERIVE:0:U
dns_request		value:DERIVE:0:U
dns_resolver		value:DERIVE:0:U
dns_response		value:DERIVE:0:U
dns_transfer		value:DERIVE:0:U
dns_update		value:DERIVE:0:U
dns_zops		value:DERIVE:0:U
duration		seconds:GAUGE:0:U
email_check		value:GAUGE:0:U
email_count		value:GAUGE:0:U
email_size		value:GAUGE:0:U
entropy			value:GAUGE:0:4294967295
fanspeed		value:GAUGE:0:U
file_size		value:GAUGE:0:U
files			value:GAUGE:0:U
fork_rate		value:DERIVE:0:U
frequency_offset	value:GAUGE:-1000000:1000000
frequency		value:GAUGE:0:U
fscache_stat		value:DERIVE:0:U
gauge			value:GAUGE:U:U
hash_collisions		value:DERIVE:0:U
http_request_methods	value:DERIVE:0:U
http_requests		value:DERIVE:0:U
http_response_codes	value:DERIVE:0:U
humidity		value:GAUGE:0:100
if_collisions		value:DERIVE:0:U
if_dropped		rx:DERIVE:0:U, tx:DERIVE:0:U
if_errors		rx:DERIVE:0:U, tx:DERIVE:0:U
if_multicast		value:DERIVE:0:U
if_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
if_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
if_rx_errors		value:DERIVE:0:U
if_rx_octets		value:DERIVE:0:U
if_tx_errors		value:DERIVE:0:U
if_tx_octets		value:DERIVE:0:U
invocations		value:DERIVE:0:U
io_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
io_packets		rx:DERIVE:0:U, tx:DERIVE:0:U
ipt_bytes		value:DERIVE:0:U
ipt_packets		value:DERIVE:0:U
irq			value:DERIVE:0:U
latency			value:GAUGE:0:U
links			value:GAUGE:0:U
load			shortterm:GAUGE:0:5000, midterm:GAUGE:0:5000, longterm:GAUGE:0:5000
md_disks		value:GAUGE:0:U
memcached_command	value:DERIVE:0:U
memcached_connections	value:GAUGE:0:U
memcached_items		value:GAUGE:0:U
memcached_octets	rx:DERIVE:0:U, tx:DERIVE:0:U
memcached_ops		value:DERIVE:0:U
memory			value:GAUGE:0:281474976710656
multimeter		value:GAUGE:U:U
mutex_operations	value:DERIVE:0:U
mysql_commands		value:DERIVE:0:U
mysql_handler		value:DERIVE:0:U
mysql_locks		value:DERIVE:0:U
mysql_log_position	value:DERIVE:0:U
mysql_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
nfs_procedure		value:DERIVE:0:U
nginx_connections	value:GAUGE:0:U
nginx_requests		value:DERIVE:0:U
node_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
node_rssi		value:GAUGE:0:255
node_stat		value:DERIVE:0:U
node_tx_rate		value:GAUGE:0:127
objects			value:GAUGE:0:U
operations		value:DERIVE:0:U
percent			value:GAUGE:0:100.1
percent_bytes		value:GAUGE:0:100.1
percent_inodes		value:GAUGE:0:100.1
pf_counters		value:DERIVE:0:U
pf_limits		value:DERIVE:0:U
pf_source		value:DERIVE:0:U
pf_states		value:GAUGE:0:U
pf_state		value:DERIVE:0:U
pg_blks			value:DERIVE:0:U
pg_db_size		value:GAUGE:0:U
pg_n_tup_c		value:DERIVE:0:U
pg_n_tup_g		value:GAUGE:0:U
pg_numbackends		value:GAUGE:0:U
pg_scan			value:DERIVE:0:U
pg_xact			value:DERIVE:0:U
ping_droprate		value:GAUGE:0:100
ping_stddev		value:GAUGE:0:65535
ping			value:GAUGE:0:65535
players			value:GAUGE:0:1000000
power			value:GAUGE:0:U
protocol_counter	value:DERIVE:0:U
ps_code			value:GAUGE:0:9223372036854775807
ps_count		processes:GAUGE:0:1000000, threads:GAUGE:0:1000000
ps_cputime		user:DERIVE:0:U, syst:DERIVE:0:U
ps_data			value:GAUGE:0:9223372036854775807
ps_disk_octets		read:DERIVE:0:U, write:DERIVE:0:U
ps_disk_ops		read:DERIVE:0:U, write:DERIVE:0:U
ps_pagefaults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
ps_rss			value:GAUGE:0:9223372036854775807
ps_stacksize		value:GAUGE:0:9223372036854775807
ps_state		value:GAUGE:0:65535
ps_vm			value:GAUGE:0:9223372036854775807
queue_length		value:GAUGE:0:U
records			value:GAUGE:0:U
requests		value:GAUGE:0:U
response_time		value:GAUGE:0:U
response_code		value:GAUGE:0:U
route_etx		value:GAUGE:0:U
route_metric		value:GAUGE:0:U
routes			value:GAUGE:0:U
serial_octets		rx:DERIVE:0:U, tx:DERIVE:0:U
signal_noise		value:GAUGE:U:0
signal_power		value:GAUGE:U:0
signal_quality		value:GAUGE:0:U
snr			value:GAUGE:0:U
spam_check		value:GAUGE:0:U
spam_score		value:GAUGE:U:U
spl			value:GAUGE:U:U
swap_io			value:DERIVE:0:U
swap			value:GAUGE:0:1099511627776
tcp_connections		value:GAUGE:0:4294967295
temperature		value:GAUGE:U:U
threads			value:GAUGE:0:U
time_dispersion		value:GAUGE:-1000000:1000000
timeleft		value:GAUGE:0:U
time_offset		value:GAUGE:-1000000:1000000
total_bytes		value:DERIVE:0:U
total_connections	value:DERIVE:0:U
total_objects		value:DERIVE:0:U
total_operations	value:DERIVE:0:U
total_requests		value:DERIVE:0:U
total_sessions		value:DERIVE:0:U
total_threads		value:DERIVE:0:U
total_time_in_ms	value:DERIVE:0:U
total_values		value:DERIVE:0:U
uptime			value:GAUGE:0:4294967295
users			value:GAUGE:0:65535
vcl			value:GAUGE:0:65535
vcpu			value:GAUGE:0:U
virt_cpu_total		value:DERIVE:0:U
virt_vcpu		value:DERIVE:0:U
vmpage_action		value:DERIVE:0:U
vmpage_faults		minflt:DERIVE:0:U, majflt:DERIVE:0:U
vmpage_io		in:DERIVE:0:U, out:DERIVE:0:U
vmpage_number		value:GAUGE:0:4294967295
volatile_changes	value:GAUGE:0:U
voltage_threshold	value:GAUGE:U:U, threshold:GAUGE:U:U
voltage			value:GAUGE:U:U
vs_memory		value:GAUGE:0:9223372036854775807
vs_processes		value:GAUGE:0:65535
vs_threads		value:GAUGE:0:65535

#
# Legacy types
# (required for the v5 upgrade target)
#
arc_counts		demand_data:COUNTER:0:U, demand_metadata:COUNTER:0:U, prefetch_data:COUNTER:0:U, prefetch_metadata:COUNTER:0:U
arc_l2_bytes		read:COUNTER:0:U, write:COUNTER:0:U
arc_l2_size		value:GAUGE:0:U
arc_ratio		value:GAUGE:0:U
arc_size		current:GAUGE:0:U, target:GAUGE:0:U, minlimit:GAUGE:0:U, maxlimit:GAUGE:0:U
mysql_qcache		hits:COUNTER:0:U, inserts:COUNTER:0:U, not_cached:COUNTER:0:U, lowmem_prunes:COUNTER:0:U, queries_in_cache:GAUGE:0:U
mysql_threads		running:GAUGE:0:U, connected:GAUGE:0:U, cached:GAUGE:0:U, created:COUNTER:0:U
`
