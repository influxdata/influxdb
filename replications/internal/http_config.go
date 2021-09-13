package internal

import "github.com/influxdata/influxdb/v2/kit/platform"

// ReplicationHTTPConfig contains all info needed by a client to make HTTP requests against the
// remote bucket targeted by a replication.
type ReplicationHTTPConfig struct {
	RemoteURL        string      `db:"remote_url"`
	RemoteToken      string      `db:"remote_api_token"`
	RemoteOrgID      platform.ID `db:"remote_org_id"`
	AllowInsecureTLS bool        `db:"allow_insecure_tls"`
	RemoteBucketID   platform.ID `db:"remote_bucket_id"`
}
