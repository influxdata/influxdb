package main_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cmd/influxd"
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

	if c.Admin.Port != 8083 {
		t.Fatalf("admin port mismatch: %v", c.Admin.Port)
	} else if c.Admin.Assets != "./admin" {
		t.Fatalf("admin assets mismatch: %v", c.Admin.Assets)
	}

	if c.HTTPAPI.Port != 0 {
		t.Fatalf("http api port mismatch: %v", c.HTTPAPI.Port)
	} else if c.HTTPAPI.SSLPort != 8087 {
		t.Fatalf("http api ssl port mismatch: %v", c.HTTPAPI.SSLPort)
	} else if c.HTTPAPI.SSLCertPath != "../cert.pem" {
		t.Fatalf("http api ssl cert path mismatch: %v", c.HTTPAPI.SSLCertPath)
	}

	if c.InputPlugins.Graphite.Enabled != false {
		t.Fatalf("graphite enabled mismatch: %v", c.InputPlugins.Graphite.Enabled)
	} else if c.InputPlugins.Graphite.Port != 2003 {
		t.Fatalf("graphite port mismatch: %v", c.InputPlugins.Graphite.Enabled)
	} else if c.InputPlugins.Graphite.Database != "" {
		t.Fatalf("graphite database mismatch: %v", c.InputPlugins.Graphite.Database)
	}

	if c.Raft.Port != 8090 {
		t.Fatalf("raft port mismatch: %v", c.Raft.Port)
	} else if c.Raft.Dir != "/tmp/influxdb/development/raft" {
		t.Fatalf("raft dir mismatch: %v", c.Raft.Dir)
	} else if time.Duration(c.Raft.Timeout) != time.Second {
		t.Fatalf("raft duration mismatch: %v", c.Raft.Timeout)
	}

	if c.Storage.Dir != "/tmp/influxdb/development/db" {
		t.Fatalf("data dir mismatch: %v", c.Storage.Dir)
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

  # Configure the graphite api
  [input_plugins.graphite]
  enabled = false
  port = 2003
  database = ""  # store graphite data in this database

  [input_plugins.udp]
  enabled = true
  port = 4444
  database = "test"

# Raft configuration
[raft]
# The raft port should be open between all servers in a cluster.
# However, this port shouldn't be accessible from the internet.

port = 8090

# Where the raft logs are stored. The user running InfluxDB will need read/write access.
dir  = "/tmp/influxdb/development/raft"

# election-timeout = "2s"

[storage]
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

# Here's an example. Note that the port on the host is the same as the raft port.
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
