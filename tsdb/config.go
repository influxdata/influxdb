package tsdb

type Config struct {
	Dir                   string   `toml:"dir"`
	RetentionAutoCreate   bool     `toml:"retention-auto-create"`
	RetentionCheckEnabled bool     `toml:"retention-check-enabled"`
	RetentionCheckPeriod  Duration `toml:"retention-check-period"`
	RetentionCreatePeriod Duration `toml:"retention-create-period"`
}
