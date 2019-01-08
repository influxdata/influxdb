package influxdb

// TODO(desa): These files are possibly a temporary. This is needed
// as a part of the source work that is being done.
// See https://github.com/influxdata/platform/issues/594 for more info.

// SourceQuery is a query for a source.
type SourceQuery struct {
	Query string `json:"query"`
	Type  string `json:"type"`
}
