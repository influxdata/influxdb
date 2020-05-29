package influxdb

// BuildInfo represents the information about InfluxDB build.
type BuildInfo struct {
	Version string // Version is the current git tag with v prefix stripped
	Commit  string // Commit is the current git commit SHA
	Date    string // Date is the build date in RFC3339
}

var buildInfo BuildInfo

// SetBuildInfo sets the build information for the binary.
func SetBuildInfo(version, commit, date string) {
	buildInfo.Version = version
	buildInfo.Commit = commit
	buildInfo.Date = date
}

// GetBuildInfo returns the current build information for the binary.
func GetBuildInfo() BuildInfo {
	return buildInfo
}
