package tsdb

import "github.com/influxdb/influxdb/toml"

type Config struct {
	Dir                   string        `toml:"dir"`
	RetentionAutoCreate   bool          `toml:"retention-auto-create"`
	RetentionCheckEnabled bool          `toml:"retention-check-enabled"`
	RetentionCheckPeriod  toml.Duration `toml:"retention-check-period"`
	RetentionCreatePeriod toml.Duration `toml:"retention-create-period"`
}
