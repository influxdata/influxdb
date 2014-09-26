package raft_test

import (
	"encoding/json"
	"net/url"
	"reflect"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that the config can be marshaled and unmarshaled.
func TestConfig_MarshalJSON(t *testing.T) {
	tests := []struct {
		c   *raft.Config
		out string
	}{
		// 0. No nodes.
		{
			c:   &raft.Config{ClusterID: 100},
			out: `{"clusterID":100}`,
		},

		// 1. One node.
		{
			c:   &raft.Config{ClusterID: 100, Nodes: []*raft.Node{&raft.Node{ID: 1, URL: &url.URL{Host: "localhost"}}}},
			out: `{"clusterID":100,"nodes":[{"id":1,"url":"//localhost"}]}`,
		},

		// 1. Node without URL.
		{
			c:   &raft.Config{ClusterID: 100, Nodes: []*raft.Node{&raft.Node{ID: 1, URL: nil}}},
			out: `{"clusterID":100,"nodes":[{"id":1}]}`,
		},
	}
	for i, tt := range tests {
		b, err := json.Marshal(tt.c)
		if err != nil {
			t.Fatalf("%d. unexpected marshaling error: %s", i, err)
		} else if string(b) != tt.out {
			t.Fatalf("%d. unexpected json: %s", i, b)
		}

		var config *raft.Config
		if err := json.Unmarshal(b, &config); err != nil {
			t.Fatalf("%d. unexpected marshaling error: %s", i, err)
		} else if !reflect.DeepEqual(tt.c, config) {
			t.Fatalf("%d. config:\n\nexp: %#v\n\ngot: %#v", i, tt.c, config)
		}
	}
}
