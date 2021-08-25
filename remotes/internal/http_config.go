package internal

import "github.com/influxdata/influxdb/v2/kit/platform"

// RemoteConnectionHTTPConfig contains all info needed by a client to make HTTP requests against a
// remote InfluxDB API.
type RemoteConnectionHTTPConfig struct {
	RemoteURL        string      `db:"remote_url"`
	RemoteToken      string      `db:"remote_api_token"`
	RemoteOrgID      platform.ID `db:"remote_org_id"`
	AllowInsecureTLS bool        `db:"allow_insecure_tls"`
}
