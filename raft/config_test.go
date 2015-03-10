package raft_test

import (
	"bytes"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/influxdb/influxdb/raft"
)

// Ensure that the config can find a node by id.
func TestConfig_NodeByID(t *testing.T) {
	c := &raft.Config{
		Nodes: []*raft.ConfigNode{
			{ID: 1, URL: url.URL{Host: "localhost:8000"}},
			{ID: 2, URL: url.URL{Host: "localhost:9000"}},
		},
	}

	// Matching nodes should return the correct node.
	if n := c.NodeByID(2); n != c.Nodes[1] {
		t.Fatalf("unexpected node: %#v", n)
	}

	// Non-existent nodes should return nil.
	if n := c.NodeByID(1000); n != nil {
		t.Fatalf("expected nil node: %#v", n)
	}
}

// Ensure that the config can find a node by URL.
func TestConfig_NodeByURL(t *testing.T) {
	c := &raft.Config{
		Nodes: []*raft.ConfigNode{
			{ID: 1, URL: url.URL{Host: "localhost:8000"}},
			{ID: 2, URL: url.URL{Host: "localhost:9000"}},
		},
	}

	// Matching nodes should return the correct node.
	if n := c.NodeByURL(url.URL{Host: "localhost:8000"}); n != c.Nodes[0] {
		t.Fatalf("unexpected node: %#v", n)
	}

	// Non-existent nodes should return nil.
	if n := c.NodeByURL(url.URL{Scheme: "http", Host: "localhost:8000"}); n != nil {
		t.Fatalf("expected nil node: %#v", n)
	}
}

// Ensure that the config can add nodes.
func TestConfig_AddNode(t *testing.T) {
	var c raft.Config
	c.AddNode(1, url.URL{Host: "localhost:8000"})
	c.AddNode(2, url.URL{Host: "localhost:9000"})
	if n := c.Nodes[0]; !reflect.DeepEqual(n, &raft.ConfigNode{ID: 1, URL: url.URL{Host: "localhost:8000"}}) {
		t.Fatalf("unexpected node(0): %#v", n)
	} else if n = c.Nodes[1]; !reflect.DeepEqual(n, &raft.ConfigNode{ID: 2, URL: url.URL{Host: "localhost:9000"}}) {
		t.Fatalf("unexpected node(1): %#v", n)
	}
}

// Ensure that the config can remove nodes.
func TestConfig_RemoveNode(t *testing.T) {
	var c raft.Config
	c.AddNode(1, url.URL{Host: "localhost:8000"})
	c.AddNode(2, url.URL{Host: "localhost:9000"})
	if err := c.RemoveNode(1); err != nil {
		t.Fatalf("unexpected error(0): %s", err)
	} else if err = c.RemoveNode(2); err != nil {
		t.Fatalf("unexpected error(1): %s", err)
	} else if err = c.RemoveNode(1000); err != raft.ErrNodeNotFound {
		t.Fatalf("unexpected error(2): %s", err)
	}
}

// Ensure that the config encoder can properly encode a config.
func TestConfigEncoder_Encode(t *testing.T) {
	c := &raft.Config{
		ClusterID: 100,
		Index:     20,
		MaxNodeID: 3,
		Nodes: []*raft.ConfigNode{
			{ID: 1, URL: url.URL{Host: "localhost:8000"}},
			{ID: 2, URL: url.URL{Host: "localhost:9000"}},
		},
	}

	var buf bytes.Buffer
	if err := raft.NewConfigEncoder(&buf).Encode(c); err != nil {
		t.Fatal(err)
	} else if buf.String() != `{"clusterID":100,"index":20,"maxNodeID":3,"nodes":[{"id":1,"url":"//localhost:8000"},{"id":2,"url":"//localhost:9000"}]}`+"\n" {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

// Ensure that the config decoder can properly decode a config.
func TestConfigDecoder_Decode(t *testing.T) {
	exp := &raft.Config{
		ClusterID: 100,
		Index:     20,
		MaxNodeID: 3,
		Nodes: []*raft.ConfigNode{
			{ID: 1, URL: url.URL{Host: "localhost:8000"}},
			{ID: 2, URL: url.URL{Host: "localhost:9000"}},
		},
	}

	var c raft.Config
	r := strings.NewReader(`{"clusterID":100,"index":20,"maxNodeID":3,"nodes":[{"id":1,"url":"//localhost:8000"},{"id":2,"url":"//localhost:9000"}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(exp, &c) {
		t.Fatalf("unexpected config: %#v", &c)
	}
}

// Ensure that the decoder returns an error on invalid node id.
func TestConfigDecoder_Decode_ErrInvalidNodeID(t *testing.T) {
	var c raft.Config
	r := strings.NewReader(`{"nodes":[{"id":0,"url":"//localhost:8000"}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err != raft.ErrInvalidNodeID {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on missing node url.
func TestConfigDecoder_Decode_ErrNodeURLRequired(t *testing.T) {
	var c raft.Config
	r := strings.NewReader(`{"nodes":[{"id":100,"url":""}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err != raft.ErrNodeURLRequired {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on duplicate node ids.
func TestConfigDecoder_Decode_ErrDuplicateNodeID(t *testing.T) {
	var c raft.Config
	r := strings.NewReader(`{"nodes":[{"id":2,"url":"//localhost:8000"},{"id":2,"url":"//localhost:9000"}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err != raft.ErrDuplicateNodeID {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on duplicate node urls.
func TestConfigDecoder_Decode_ErrDuplicateNodeURL(t *testing.T) {
	var c raft.Config
	r := strings.NewReader(`{"nodes":[{"id":1,"url":"//localhost:8000"},{"id":2,"url":"//localhost:8000"}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err != raft.ErrDuplicateNodeURL {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on invalid JSON.
func TestConfigDecoder_Decode_ErrUnexpectedEOF(t *testing.T) {
	var c raft.Config
	if err := raft.NewConfigDecoder(strings.NewReader(`{"no`)).Decode(&c); err == nil || err.Error() != "unexpected EOF" {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that the decoder returns an error on an invalid URL.
func TestConfigDecoder_Decode_ErrInvalidURL(t *testing.T) {
	var c raft.Config
	r := strings.NewReader(`{"nodes":[{"id":0,"url":"//my%host"}]}`)
	if err := raft.NewConfigDecoder(r).Decode(&c); err == nil || err.Error() != "parse //my%host: hexadecimal escape in host" {
		t.Fatalf("unexpected error: %s", err)
	}
}
