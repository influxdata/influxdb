package monitor

import "github.com/influxdata/influxdb/monitor/diagnostics"

// build holds information of the build of the current executable.
type build struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

func (b *build) Diagnostics() (*diagnostics.Diagnostics, error) {
	d := map[string]interface{}{
		"Version":    b.Version,
		"Commit":     b.Commit,
		"Branch":     b.Branch,
		"Build Time": b.Time,
	}

	return diagnostics.RowFromMap(d), nil
}
