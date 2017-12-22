package tsdb

// DefaultMaxSeriesFileSize is the maximum series file size. Assuming that each
// series key takes, for example, 150 bytes, the limit would support ~900K series.
const DefaultMaxSeriesFileSize = 128 * (1 << 20) // 128MB
