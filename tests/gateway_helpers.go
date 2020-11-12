package tests

// WritePath is the path to write influx 2.x points.
const WritePath = "/api/v2/write"

// WritePathLegacy is the path to write influx 1.x points.
const WritePathLegacy = "/write"

// Default values created when calling NewDefaultGatewayNode.
const (
	DefaultOrgName = "myorg"

	DefaultBucketName = "db/rp" // Since we can only write data via 1.x path we need to have a 1.x bucket name

	DefaultUsername = "admin"
	DefaultPassword = "password"

	// DefaultToken has permissions to write to DefaultBucket
	DefaultToken = "mytoken"
	// OperToken has permissions to do anything.
	OperToken = "opertoken"

	DefaultDatabase        = "db"
	DefaultRetentionPolicy = "rp"
)
