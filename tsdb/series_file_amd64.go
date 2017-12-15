package tsdb

// DefaultMaxSeriesFileSize is the maximum series file size. Assuming that each
// series key takes, for example, 150 bytes, the limit would support ~229M series.
const DefaultMaxSeriesFileSize = 32 * (1 << 30) // 32GB
