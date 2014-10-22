package influxdb

import (
	"time"
)

const (
	DefaultShardSpaceName = "default"

	DefaultSpaceSplit              = 1
	DefaultReplicationFactor       = 1
	DefaultShardDuration           = 7 * (24 * time.Hour)
	DefaultRetentionPolicyDuration = 0

	// DefaultRootPassword is the password initially set for the root user.
	// It is also used when reseting the root user's password.
	DefaultRootPassword = "root"
)
