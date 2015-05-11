package raft

import (
	"encoding/json"
	"io"
	"net/url"
)

// Config represents the configuration for the log.
type Config struct {
	// Cluster identifier. Used to prevent separate clusters from
	// accidentally communicating with one another.
	ClusterID uint64

	// Index is the last log index when the configuration was updated.
	Index uint64

	// MaxNodeID is the largest node identifier generated for this config.
	MaxNodeID uint64

	// List of nodes in the cluster.
	Nodes []*ConfigNode
}

// NodeByID returns a node by identifier.
func (c *Config) NodeByID(id uint64) *ConfigNode {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// NodeByURL returns a node by URL.
func (c *Config) NodeByURL(u url.URL) *ConfigNode {
	for _, n := range c.Nodes {
		if n.URL.String() == u.String() {
			return n
		}
	}
	return nil
}

// AddNode adds a new node to the config.
func (c *Config) AddNode(id uint64, u url.URL) error {
	// Validate that the id is non-zero and the url exists.
	if id == 0 {
		return ErrInvalidNodeID
	} else if u.Host == "" {
		return ErrNodeURLRequired
	}

	// Validate that no other nodes in the config have the same id or URL.
	for _, n := range c.Nodes {
		if n.ID == id {
			return ErrDuplicateNodeID
		} else if n.URL.String() == u.String() {
			return ErrDuplicateNodeURL
		}
	}

	// Add the node to the config.
	c.Nodes = append(c.Nodes, &ConfigNode{ID: id, URL: u})

	return nil
}

// RemoveNode removes a node by id.
// Returns ErrNodeNotFound if the node does not exist.
func (c *Config) RemoveNode(id uint64) error {
	for i, node := range c.Nodes {
		if node.ID == id {
			copy(c.Nodes[i:], c.Nodes[i+1:])
			c.Nodes[len(c.Nodes)-1] = nil
			c.Nodes = c.Nodes[:len(c.Nodes)-1]
			return nil
		}
	}
	return ErrNodeNotFound
}

// Clone returns a deep copy of the configuration.
func (c *Config) Clone() *Config {
	other := &Config{
		ClusterID: c.ClusterID,
		Index:     c.Index,
		MaxNodeID: c.MaxNodeID,
	}
	other.Nodes = make([]*ConfigNode, len(c.Nodes))
	for i, n := range c.Nodes {
		other.Nodes[i] = n.clone()
	}
	return other
}

// ConfigNode represents a single machine in the raft configuration.
type ConfigNode struct {
	ID                  uint64
	URL                 url.URL
	LastHeartbeatError  int64 // the time a heartbeat returned an error
	HeartbeatErrorCount int64 // the number of times in a row that a heartbeat has failed
}

// clone returns a deep copy of the node.
func (n *ConfigNode) clone() *ConfigNode {
	other := &ConfigNode{ID: n.ID}
	other.URL = n.URL
	return other
}

// ConfigEncoder encodes a config to a writer.
type ConfigEncoder struct {
	w io.Writer
}

// NewConfigEncoder returns a new instance of ConfigEncoder attached to a writer.
func NewConfigEncoder(w io.Writer) *ConfigEncoder {
	return &ConfigEncoder{w}
}

// Encode marshals the configuration to the encoder's writer.
func (enc *ConfigEncoder) Encode(c *Config) error {
	// Copy properties to intermediate type.
	var o configJSON
	o.ClusterID = c.ClusterID
	o.Index = c.Index
	o.MaxNodeID = c.MaxNodeID
	for _, n := range c.Nodes {
		o.Nodes = append(o.Nodes, &configNodeJSON{ID: n.ID, URL: n.URL.String()})
	}

	// Encode intermediate type as JSON.
	return json.NewEncoder(enc.w).Encode(&o)
}

// ConfigDecoder decodes a config from a reader.
type ConfigDecoder struct {
	r io.Reader
}

// NewConfigDecoder returns a new instance of ConfigDecoder attached to a reader.
func NewConfigDecoder(r io.Reader) *ConfigDecoder {
	return &ConfigDecoder{r}
}

// Decode marshals the configuration to the decoder's reader.
func (dec *ConfigDecoder) Decode(c *Config) error {
	// Decode into intermediate type.
	var o configJSON
	if err := json.NewDecoder(dec.r).Decode(&o); err != nil {
		return err
	}

	// Copy properties to config.
	c.ClusterID = o.ClusterID
	c.Index = o.Index
	c.MaxNodeID = o.MaxNodeID

	// Validate and append nodes.
	for _, n := range o.Nodes {
		// Parse node URL.
		u, err := url.Parse(n.URL)
		if err != nil {
			return err
		} else if n.URL == "" {
			u = &url.URL{}
		}

		// Append node to config.
		if err := c.AddNode(n.ID, *u); err != nil {
			return err
		}
	}

	return nil
}

// configJSON represents an intermediate struct used for JSON encoding.
type configJSON struct {
	ClusterID uint64            `json:"clusterID,omitempty"`
	Index     uint64            `json:"index,omitempty"`
	MaxNodeID uint64            `json:"maxNodeID,omitempty"`
	Nodes     []*configNodeJSON `json:"nodes,omitempty"`
}

// configNodeJSON represents the JSON serialized form of the ConfigNode type.
type configNodeJSON struct {
	ID  uint64 `json:"id"`
	URL string `json:"url,omitempty"`
}
