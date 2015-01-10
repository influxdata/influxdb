package main_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	main "github.com/influxdb/influxdb/cmd/influxd"
)

// Ensure that megabyte sizes can be parsed.
func TestSize_UnmarshalText_MB(t *testing.T) {
	var s main.Size
	if err := s.UnmarshalText([]byte("200m")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if s != 200*(1<<20) {
		t.Fatalf("unexpected size: %d", s)
	}
}

// Ensure that gigabyte sizes can be parsed.
func TestSize_UnmarshalText_GB(t *testing.T) {
	if typ := reflect.TypeOf(0); typ.Size() != 8 {
		t.Skip("large gigabyte parsing on 64-bit arch only")
	}

	var s main.Size
	if err := s.UnmarshalText([]byte("10g")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if s != 10*(1<<30) {
		t.Fatalf("unexpected size: %d", s)
	}
}

// Ensure that a TOML configuration file can be parsed into a Config.
func TestParseConfig(t *testing.T) {
	c, err := main.ParseConfig(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if c.Hostname != "myserver.com" {
		t.Fatalf("hostname mismatch: %v", c.Hostname)
	}

	if c.Logging.File != "influxdb.log" {
		t.Fatalf("logging file mismatch: %v", c.Logging.File)
	} else if c.Logging.Level != "info" {
		t.Fatalf("logging level mismatch: %v", c.Logging.Level)
	}

	if !c.Authentication.Enabled {
		t.Fatalf("authentication enabled mismatch: %v", c.Authentication.Enabled)
	}

	if c.Admin.Port != 8083 {
		t.Fatalf("admin port mismatch: %v", c.Admin.Port)
	} else if c.Admin.Assets != "./admin" {
		t.Fatalf("admin assets mismatch: %v", c.Admin.Assets)
	}

	if c.HTTPAPI.Port != main.DefaultBrokerPort {
		t.Fatalf("http api port mismatch: %v", c.HTTPAPI.Port)
	} else if c.HTTPAPI.SSLPort != 8087 {
		t.Fatalf("http api ssl port mismatch: %v", c.HTTPAPI.SSLPort)
	} else if c.HTTPAPI.SSLCertPath != "../cert.pem" {
		t.Fatalf("http api ssl cert path mismatch: %v", c.HTTPAPI.SSLCertPath)
	}

	if len(c.Graphites) != 2 {
		t.Fatalf("graphites  mismatch.  expected %v, got: %v", 2, len(c.Graphites))
	}

	tcpGraphite := c.Graphites[0]
	switch {
	case tcpGraphite.Enabled != true:
		t.Fatalf("graphite tcp enabled mismatch: expected: %v, got %v", true, tcpGraphite.Enabled)
	case tcpGraphite.Addr != "192.168.0.1":
		t.Fatalf("graphite tcp address mismatch: expected %v, got  %v", "192.168.0.1", tcpGraphite.Addr)
	case tcpGraphite.Port != 2003:
		t.Fatalf("graphite tcp port mismatch: expected %v, got %v", 2003, tcpGraphite.Port)
	case tcpGraphite.Database != "graphite_tcp":
		t.Fatalf("graphite tcp database mismatch: expected %v, got %v", "graphite_tcp", tcpGraphite.Database)
	case strings.ToLower(tcpGraphite.Protocol) != "tcp":
		t.Fatalf("graphite tcp protocol mismatch: expected %v, got %v", "tcp", strings.ToLower(tcpGraphite.Protocol))
	case tcpGraphite.LastEnabled() != true:
		t.Fatalf("graphite tcp name-position mismatch: expected %v, got %v", "last", tcpGraphite.NamePosition)
	case tcpGraphite.NameSeparatorString() != "-":
		t.Fatalf("graphite tcp name-separator mismatch: expected %v, got %v", "-", tcpGraphite.NameSeparatorString())
	}

	udpGraphite := c.Graphites[1]
	switch {
	case udpGraphite.Enabled != true:
		t.Fatalf("graphite udp enabled mismatch: expected: %v, got %v", true, udpGraphite.Enabled)
	case udpGraphite.Addr != "192.168.0.2":
		t.Fatalf("graphite udp address mismatch: expected %v, got  %v", "192.168.0.2", udpGraphite.Addr)
	case udpGraphite.Port != 2005:
		t.Fatalf("graphite udp port mismatch: expected %v, got %v", 2005, udpGraphite.Port)
	case udpGraphite.Database != "graphite_udp":
		t.Fatalf("graphite database mismatch: expected %v, got %v", "graphite_udp", udpGraphite.Database)
	case strings.ToLower(udpGraphite.Protocol) != "udp":
		t.Fatalf("graphite udp protocol mismatch: expected %v, got %v", "udp", strings.ToLower(udpGraphite.Protocol))
	}

	switch {
	case c.Collectd.Enabled != true:
		t.Errorf("collectd enabled mismatch: expected: %v, got %v", true, c.Collectd.Enabled)
	case c.Collectd.Addr != "192.168.0.3":
		t.Errorf("collectd address mismatch: expected %v, got  %v", "192.168.0.3", c.Collectd.Addr)
	case c.Collectd.Port != 25827:
		t.Errorf("collectd port mismatch: expected %v, got %v", 2005, c.Collectd.Port)
	case c.Collectd.Database != "collectd_database":
		t.Errorf("collectdabase mismatch: expected %v, got %v", "collectd_database", c.Collectd.Database)
	case c.Collectd.TypesDB != "foo-db-type":
		t.Errorf("collectd typesdb mismatch: expected %v, got %v", "foo-db-type", c.Collectd.TypesDB)
	}

	if c.Broker.Port != 8090 {
		t.Fatalf("broker port mismatch: %v", c.Broker.Port)
	} else if c.Broker.Dir != "/tmp/influxdb/development/broker" {
		t.Fatalf("broker dir mismatch: %v", c.Broker.Dir)
	} else if time.Duration(c.Broker.Timeout) != time.Second {
		t.Fatalf("broker duration mismatch: %v", c.Broker.Timeout)
	}

	if c.Data.Dir != "/tmp/influxdb/development/db" {
		t.Fatalf("data dir mismatch: %v", c.Data.Dir)
	}

	if c.Cluster.ProtobufPort != 8099 {
		t.Fatalf("protobuf port mismatch: %v", c.Cluster.ProtobufPort)
	} else if time.Duration(c.Cluster.ProtobufTimeout) != 2*time.Second {
		t.Fatalf("protobuf timeout mismatch: %v", c.Cluster.ProtobufTimeout)
	} else if time.Duration(c.Cluster.ProtobufHeartbeatInterval) != 200*time.Millisecond {
		t.Fatalf("protobuf heartbeat interval mismatch: %v", c.Cluster.ProtobufHeartbeatInterval)
	} else if time.Duration(c.Cluster.MinBackoff) != 100*time.Millisecond {
		t.Fatalf("min backoff mismatch: %v", c.Cluster.MinBackoff)
	} else if time.Duration(c.Cluster.MaxBackoff) != 1*time.Second {
		t.Fatalf("max backoff mismatch: %v", c.Cluster.MaxBackoff)
	} else if c.Cluster.MaxResponseBufferSize != 5 {
		t.Fatalf("max response buffer size mismatch: %v", c.Cluster.MaxResponseBufferSize)
	}

	// TODO: UDP Servers testing.
	/*
		c.Assert(config.UdpServers, HasLen, 1)
		c.Assert(config.UdpServers[0].Enabled, Equals, true)
		c.Assert(config.UdpServers[0].Port, Equals, 4444)
		c.Assert(config.UdpServers[0].Database, Equals, "test")
	*/
}

// Testing configuration file.
const testFile = `
# Welcome to the InfluxDB configuration file.

# If hostname (on the OS) doesn't return a name that can be resolved by the other
# systems in the cluster, you'll have to set the hostname to an IP or something
# that can be resolved here.
hostname = "myserver.com"

# Control authentication
[authentication]
enabled = true

[logging]
# logging level can be one of "debug", "info", "warn" or "error"
level  = "info"
file   = "influxdb.log"

# Configure the admin server
[admin]
port   = 8083                   # binding is disabled if the port isn't set
assets = "./admin"

# Configure the http api
[api]
ssl-port = 8087    # Ssl support is enabled if you set a port and cert
ssl-cert = "../cert.pem"

# connections will timeout after this amount of time. Ensures that clients that misbehave
# and keep alive connections they don't use won't end up connection a million times.
# However, if a request is taking longer than this to complete, could be a problem.
read-timeout = "5s"

[input_plugins]

  [input_plugins.udp]
  enabled = true
  port = 4444
  database = "test"

# Configure the Graphite servers
[[graphite]]
protocol = "TCP"
enabled = true
address = "192.168.0.1"
port = 2003
database = "graphite_tcp"  # store graphite data in this database
name-position = "last"
name-separator = "-"

[[graphite]]
protocol = "udP"
enabled = true
address = "192.168.0.2"
port = 2005
database = "graphite_udp"  # store graphite data in this database

# Configure collectd server
[collectd]
enabled = true
address = "192.168.0.3"
port = 25827
database = "collectd_database"
typesdb = "foo-db-type"

# Raft configuration
[raft]
# The raft port should be open between all servers in a cluster.
# Broker configuration
[broker]
# The broker port should be open between all servers in a cluster.
# However, this port shouldn't be accessible from the internet.

port = 8090

# Where the broker logs are stored. The user running InfluxDB will need read/write access.
dir  = "/tmp/influxdb/development/broker"

# election-timeout = "2s"

[data]
dir = "/tmp/influxdb/development/db"

# How many requests to potentially buffer in memory. If the buffer gets filled then writes
# will still be logged and once the local storage has caught up (or compacted) the writes
# will be replayed from the WAL
write-buffer-size = 10000

# The server will check this often for shards that have expired and should be cleared.
retention-sweep-period = "10m"

[cluster]
# A comma separated list of servers to seed
# this server. this is only relevant when the
# server is joining a new cluster. Otherwise
# the server will use the list of known servers
# prior to shutting down. Any server can be pointed to
# as a seed. It will find the Raft leader automatically.

# Here's an example. Note that the port on the host is the same as the broker port.
seed-servers = ["hosta:8090", "hostb:8090"]

# Replication happens over a TCP connection with a Protobuf protocol.
# This port should be reachable between all servers in a cluster.
# However, this port shouldn't be accessible from the internet.

protobuf_port = 8099
protobuf_timeout = "2s" # the write timeout on the protobuf conn any duration parseable by time.ParseDuration
protobuf_heartbeat = "200ms" # the heartbeat interval between the servers. must be parseable by time.ParseDuration
protobuf_min_backoff = "100ms" # the minimum backoff after a failed heartbeat attempt
protobuf_max_backoff = "1s" # the maxmimum backoff after a failed heartbeat attempt

# How many write requests to potentially buffer in memory per server. If the buffer gets filled then writes
# will still be logged and once the server has caught up (or come back online) the writes
# will be replayed from the WAL
write-buffer-size = 10000

# the maximum number of responses to buffer from remote nodes, if the
# expected number of responses exceed this number then querying will
# happen sequentially and the buffer size will be limited to this
# number
max-response-buffer-size = 5

# When queries get distributed out to shards, they go in parallel. This means that results can get buffered
# in memory since results will come in any order, but have to be processed in the correct time order.
# Setting this higher will give better performance, but you'll need more memory. Setting this to 1 will ensure
# that you don't need to buffer in memory, but you won't get the best performance.
concurrent-shard-query-limit = 10

[leveldb]

# Maximum mmap open files, this will affect the virtual memory used by
# the process
# max-open-files = 40
lru-cache-size = "200m"

# The default setting on this is 0, which means unlimited. Set this to
# something if you want to limit the max number of open
# files. max-open-files is per shard so this * that will be max.
# max-open-shards = 0

# The default setting is 100. This option tells how many points will be fetched from LevelDb before
# they get flushed into backend.
point-batch-size = 50
`

func Test_Collectd_ConnectionString(t *testing.T) {
	var tests = []struct {
		name             string
		defaultBindAddr  string
		connectionString string
		config           main.Collectd
	}{
		{
			name:             "No address or port provided from config",
			defaultBindAddr:  "192.168.0.1",
			connectionString: "192.168.0.1:25826",
			config:           main.Collectd{},
		},
		{
			name:             "address provided, no port provided from config",
			defaultBindAddr:  "192.168.0.1",
			connectionString: "192.168.0.2:25826",
			config:           main.Collectd{Addr: "192.168.0.2"},
		},
		{
			name:             "no address provided, port provided from config",
			defaultBindAddr:  "192.168.0.1",
			connectionString: "192.168.0.1:25827",
			config:           main.Collectd{Port: 25827},
		},
		{
			name:             "both address and port provided from config",
			defaultBindAddr:  "192.168.0.1",
			connectionString: "192.168.0.2:25827",
			config:           main.Collectd{Addr: "192.168.0.2", Port: 25827},
		},
	}

	for _, test := range tests {
		t.Logf("test: %q", test.name)
		s := test.config.ConnectionString(test.defaultBindAddr)
		if s != test.connectionString {
			t.Errorf("connection string mismatch, expected: %q, got: %q", test.connectionString, s)
		}
	}
}
