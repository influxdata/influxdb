package tests

// Default values created when calling NewPipeline.
const (
	DefaultOrgName = "myorg"

	DefaultBucketName = "db/rp" // Since we can only write data via 1.x path we need to have a 1.x bucket name

	DefaultUsername = "admin"
	DefaultPassword = "password"

	// OperToken has permissions to do anything.
	OperToken = "opertoken"
)

// VeryVerbose when set to true, will enable very verbose logging of services.
var VeryVerbose bool = true
