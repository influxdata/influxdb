package influxql

// Config modifies the behavior of the Transpiler.
type Config struct {
	DefaultDatabase        string
	DefaultRetentionPolicy string
}
