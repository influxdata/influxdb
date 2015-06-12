package main

import (
	"collectd.org/api"
	"collectd.org/network"

	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	conn, err := network.Dial(net.JoinHostPort("localhost", "25826"), network.ClientOptions{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	vl := api.ValueList{
		Identifier: api.Identifier{
			Host:   "example.com",
			Plugin: "golang",
			Type:   "gauge",
		},
		Time:     time.Now(),
		Interval: 10 * time.Second,
		Values:   []api.Value{api.Gauge(42.0)},
	}
	if err := conn.Write(vl); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
