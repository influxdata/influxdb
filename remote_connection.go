package influxdb

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
)

// RemoteConnection contains all info about a remote InfluxDB instance that should be returned to users.
// Note that the auth token used by the request is *not* included here.
type RemoteConnection struct {
	ID               platform.ID  `json:"id" db:"id"`
	OrgID            platform.ID  `json:"orgID" db:"org_id"`
	Name             string       `json:"name" db:"name"`
	Description      *string      `json:"description,omitempty" db:"description"`
	RemoteURL        string       `json:"remoteURL" db:"remote_url"`
	RemoteOrgID      *platform.ID `json:"remoteOrgID" db:"remote_org_id"`
	AllowInsecureTLS bool         `json:"allowInsecureTLS" db:"allow_insecure_tls"`
}

// RemoteConnectionListFilter is a selection filter for listing remote InfluxDB instances.
type RemoteConnectionListFilter struct {
	OrgID     platform.ID
	Name      *string
	RemoteURL *string
}

// RemoteConnections is a collection of metadata about remote InfluxDB instances.
type RemoteConnections struct {
	Remotes []RemoteConnection `json:"remotes"`
}

// CreateRemoteConnectionRequest contains all info needed to establish a new connection to a remote
// InfluxDB instance.
type CreateRemoteConnectionRequest struct {
	OrgID            platform.ID  `json:"orgID"`
	Name             string       `json:"name"`
	Description      *string      `json:"description,omitempty"`
	RemoteURL        string       `json:"remoteURL"`
	RemoteToken      string       `json:"remoteAPIToken"`
	RemoteOrgID      *platform.ID `json:"remoteOrgID"`
	AllowInsecureTLS bool         `json:"allowInsecureTLS"`
}

// UpdateRemoteConnectionRequest contains a partial update to existing info about a remote InfluxDB instance.
type UpdateRemoteConnectionRequest struct {
	Name             *string      `json:"name,omitempty"`
	Description      *string      `json:"description,omitempty"`
	RemoteURL        *string      `json:"remoteURL,omitempty"`
	RemoteToken      *string      `json:"remoteAPIToken,omitempty"`
	RemoteOrgID      *platform.ID `json:"remoteOrgID,omitempty"`
	AllowInsecureTLS *bool        `json:"allowInsecureTLS,omitempty"`
}
