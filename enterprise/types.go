package enterprise

// Cluster is a collection of data nodes and non-data nodes within a
// Plutonium cluster.
type Cluster struct {
	DataNodes []DataNode `json:"data"`
	MetaNodes []Node     `json:"meta"`
}

// DataNode represents a data node in an Influx Enterprise Cluster
type DataNode struct {
	ID         uint64 `json:"id"`               // Meta store ID.
	TCPAddr    string `json:"tcpAddr"`          // RPC addr, e.g., host:8088.
	HTTPAddr   string `json:"httpAddr"`         // Client addr, e.g., host:8086.
	HTTPScheme string `json:"httpScheme"`       // "http" or "https" for HTTP addr.
	Status     string `json:"status,omitempty"` // The cluster status of the node.
}

// Node represent any meta or data node in an Influx Enterprise cluster
type Node struct {
	ID         uint64 `json:"id"`
	Addr       string `json:"addr"`
	HTTPScheme string `json:"httpScheme"`
	TCPAddr    string `json:"tcpAddr"`
}

// Permissions maps resources to a set of permissions.
// Specifically, it maps a database to a set of permissions
type Permissions map[string][]string

// User represents an enterprise user.
type User struct {
	Name        string      `json:"name"`
	Password    string      `json:"password,omitempty"`
	Permissions Permissions `json:"permissions,omitempty"`
}

// Users represents a set of enterprise users.
type Users struct {
	Users []User `json:"users,omitempty"`
}

// UserAction represents and action to be taken with a user.
type UserAction struct {
	Action string `json:"action"`
	User   *User  `json:"user"`
}

// Role is a restricted set of permissions assigned to a set of users.
type Role struct {
	Name        string      `json:"name"`
	NewName     string      `json:"newName,omitempty"`
	Permissions Permissions `json:"permissions,omitempty"`
	Users       []string    `json:"users,omitempty"`
}

// Roles is a set of roles
type Roles struct {
	Roles []Role `json:"roles,omitempty"`
}

// RoleAction represents an action to be taken with a role.
type RoleAction struct {
	Action string `json:"action"`
	Role   *Role  `json:"role"`
}

// Error is JSON error message return by Influx Enterprise's meta API.
type Error struct {
	Error string `json:"error"`
}
