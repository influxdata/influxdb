package tracked

import "github.com/influxdata/influxdb/v2/kit/platform"

// Replication defines a replication stream which is currently being tracked via sqlite.
type Replication struct {
	MaxQueueSizeBytes int64
	OrgID             platform.ID
	LocalBucketID     platform.ID
}
