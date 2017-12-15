package tsdb

// DefaultMaxSeriesFileSize is the maximum series file size. Assuming that each
// series key takes, for example, 150 bytes, the limit would support ~3.5M series.
const DefaultMaxSeriesFileSize = (1 << 29) // 512MB
