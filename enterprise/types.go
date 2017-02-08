package enterprise

// Cluster is a collection of data nodes and non-data nodes within a
// Plutonium cluster.
type Cluster struct {
	DataNodes []DataNode `json:"data"`
	MetaNodes []Node     `json:"meta"`
}

type DataNode struct {
	ID         uint64 `json:"id"`               // Meta store ID.
	TCPAddr    string `json:"tcpAddr"`          // RPC addr, e.g., host:8088.
	HTTPAddr   string `json:"httpAddr"`         // Client addr, e.g., host:8086.
	HTTPScheme string `json:"httpScheme"`       // "http" or "https" for HTTP addr.
	Status     string `json:"status,omitempty"` // The cluster status of the node.
}

type Node struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	HTTPScheme string `json:"httpScheme"`
	TCPAddr    string `json:"tcpAddr"`
}
