package main

import (
	"fmt"
	"net"
	"testing"
)

type MockListener struct {
	config Config
}

func (m MockListener) Accept() (net.Conn, error) {
	return nil, nil
}

func (m MockListener) Close() error {
	return nil
}

func (m MockListener) Addr() net.Addr {
	addr, _ := net.ResolveTCPAddr("tcp", m.config.ClusterAddr())
	return addr
}

func TestNodeHostParse(t *testing.T) {
	c := Config{}
	c.Port = 12345
	c.BindAddress = "192.168.0.1"
	n := Node{}
	n.clusterListener = MockListener{config: c}

	url := n.ClusterURL()
	fmt.Println(url.Host, "LALALAL")

	// If hostname is not set bind-address should be used
	if url.Host != "192.168.0.1:12345" {
		t.Fatal("Error parsing bind address")
	}

	n.hostname = "testhost"

	url = n.ClusterURL()

	// Hostname prevails over bind-address
	if url.Host != "testhost:12345" {
		t.Fatal("Error parsing host address")
	}
}
