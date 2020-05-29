package seriesfile

const (
	// DefaultLargeSeriesWriteThreshold is the number of series per write
	// that requires the series index be pregrown before insert.
	DefaultLargeSeriesWriteThreshold = 10000
)

// Config contains all of the configuration related to tsdb.
type Config struct {
	// LargeSeriesWriteThreshold is the threshold before a write requires
	// preallocation to improve throughput. Currently used in the series file.
	LargeSeriesWriteThreshold int `toml:"large-series-write-threshold"`
}

// NewConfig return a new instance of config with default settings.
func NewConfig() Config {
	return Config{
		LargeSeriesWriteThreshold: DefaultLargeSeriesWriteThreshold,
	}
}
