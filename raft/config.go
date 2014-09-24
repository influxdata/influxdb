package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

// Config represents the configuration for the log.
type Config struct {
	// Cluster identifier. Used to prevent separate clusters from
	// accidentally communicating with one another.
	ClusterID uint64 `json:"clusterID,omitempty"`

	// List of nodes in the cluster.
	Nodes []*Node `json:"nodes,omitempty"`

	// Index is the last log index when the configuration was updated.
	Index uint64 `json:"index,omitempty"`

	// MaxNodeID is the largest node identifier generated for this config.
	MaxNodeID uint64 `json:"maxNodeID,omitempty"`
}

// NodeByID returns a node by identifier.
func (c *Config) NodeByID(id uint64) *Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// NodeByURL returns a node by URL.
func (c *Config) NodeByURL(u *url.URL) *Node {
	for _, n := range c.Nodes {
		if n.URL.String() == u.String() {
			return n
		}
	}
	return nil
}

// addNode adds a new node to the config.
// Returns an error if a node with the same id or url exists.
func (c *Config) addNode(id uint64, u *url.URL) error {
	if id <= 0 {
		return errors.New("invalid node id")
	} else if u == nil {
		return errors.New("node url required")
	}

	for _, n := range c.Nodes {
		if n.ID == id {
			return fmt.Errorf("node id already exists")
		} else if n.URL.String() == u.String() {
			return fmt.Errorf("node url already in use")
		}
	}

	c.Nodes = append(c.Nodes, &Node{ID: id, URL: u})

	return nil
}

// removeNode removes a node by id.
// Returns an error if the node does not exist.
func (c *Config) removeNode(id uint64) error {
	for i, node := range c.Nodes {
		if node.ID == id {
			copy(c.Nodes[i:], c.Nodes[i+1:])
			c.Nodes[len(c.Nodes)-1] = nil
			c.Nodes = c.Nodes[:len(c.Nodes)-1]
			return nil
		}
	}
	return fmt.Errorf("node not found: %d", id)
}

// clone returns a deep copy of the configuration.
func (c *Config) clone() *Config {
	other := &Config{
		ClusterID: c.ClusterID,
		Index:     c.Index,
		MaxNodeID: c.MaxNodeID,
	}
	other.Nodes = make([]*Node, len(c.Nodes))
	for i, n := range c.Nodes {
		other.Nodes[i] = n.clone()
	}
	return other
}

// Node represents a single machine in the raft cluster.
type Node struct {
	ID  uint64   `json:"id"`
	URL *url.URL `json:"url,omitempty"`
}

// clone returns a deep copy of the node.
func (n *Node) clone() *Node {
	other := &Node{ID: n.ID, URL: &url.URL{}}
	*other.URL = *n.URL
	return other
}

// nodeJSONMarshaler represents the JSON serialized form of the Node type.
type nodeJSONMarshaler struct {
	ID  uint64 `json:"id"`
	URL string `json:"url,omitempty"`
}

// MarshalJSON encodes the node into a JSON-formatted byte slice.
func (n *Node) MarshalJSON() ([]byte, error) {
	var o nodeJSONMarshaler
	o.ID = n.ID
	if n.URL != nil {
		o.URL = n.URL.String()
	}
	return json.Marshal(&o)
}

// UnmarshalJSON decodes a JSON-formatted byte slice into a node.
func (n *Node) UnmarshalJSON(data []byte) error {
	// Unmarshal into temporary type.
	var o nodeJSONMarshaler
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}

	// Convert values to a node.
	n.ID = o.ID
	if o.URL != "" {
		u, err := url.Parse(o.URL)
		if err != nil {
			return err
		}
		n.URL = u
	}

	return nil
}
