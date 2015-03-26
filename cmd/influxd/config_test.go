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

	if c.JoinURLs() != "http://127.0.0.1:8086" {
		t.Fatalf("JoinURLs mistmatch: %v", c.JoinURLs())
	}

	if !c.Authentication.Enabled {
		t.Fatalf("authentication enabled mismatch: %v", c.Authentication.Enabled)
	}

	if c.UDP.Enabled {
		t.Fatalf("udp enabled mismatch: %v", c.UDP.Enabled)
	}

	if c.Admin.Enabled != true {
		t.Fatalf("admin enabled mismatch: %v", c.Admin.Enabled)
	}

	if c.Admin.Port != 8083 {
		t.Fatalf("admin port mismatch: %v", c.Admin.Port)
	}

	if c.ContinuousQuery.Disable == true {
		t.Fatalf("continuous query disable mismatch: %v", c.ContinuousQuery.Disable)
	}

	if c.Data.Port != main.DefaultBrokerPort {
		t.Fatalf("data port mismatch: %v", c.Data.Port)
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
	case udpGraphite.DatabaseString() != "graphite":
		t.Fatalf("graphite database mismatch: expected %v, got %v", "graphite", udpGraphite.Database)
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

	if c.Broker.Port != 8086 {
		t.Fatalf("broker port mismatch: %v", c.Broker.Port)
	} else if c.Broker.Dir != "/tmp/influxdb/development/broker" {
		t.Fatalf("broker dir mismatch: %v", c.Broker.Dir)
	} else if time.Duration(c.Broker.Timeout) != time.Second {
		t.Fatalf("broker duration mismatch: %v", c.Broker.Timeout)
	}

	if c.Data.Dir != "/tmp/influxdb/development/db" {
		t.Fatalf("data dir mismatch: %v", c.Data.Dir)
	}
	if c.Data.RetentionCheckEnabled != true {
		t.Fatalf("Retention check enabled mismatch: %v", c.Data.RetentionCheckEnabled)
	}
	if c.Data.RetentionCheckPeriod != main.Duration(5*time.Minute) {
		t.Fatalf("Retention check period mismatch: %v", c.Data.RetentionCheckPeriod)
	}

	if c.Cluster.Dir != "/tmp/influxdb/development/cluster" {
		t.Fatalf("cluster dir mismatch: %v", c.Cluster.Dir)
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

# Controls certain parameters that only take effect until an initial successful
# start-up has occurred.
[initialization]
join-urls = "http://127.0.0.1:8086"

# Control authentication
[authentication]
enabled = true

[logging]
write-tracing = true
raft-tracing = true

[statistics]
enabled = true
database = "_internal"
retention-policy = "default"
write-interval = "1m"

# Configure the admin server
[admin]
enabled = true
port = 8083

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

# Configure collectd server
[collectd]
enabled = true
address = "192.168.0.3"
port = 25827
database = "collectd_database"
typesdb = "foo-db-type"

# Broker configuration
[broker]
# The broker port should be open between all servers in a cluster.
# However, this port shouldn't be accessible from the internet.
port = 8086

# Where the broker logs are stored. The user running InfluxDB will need read/write access.
dir  = "/tmp/influxdb/development/broker"

# election-timeout = "2s"

[data]
dir = "/tmp/influxdb/development/db"
retention-auto-create = false
retention-check-enabled = true
retention-check-period = "5m"

[continuous_queries]
disable = false

[cluster]
dir = "/tmp/influxdb/development/cluster"
`

func TestCollectd_ConnectionString(t *testing.T) {
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
