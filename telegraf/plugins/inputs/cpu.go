package inputs

// CPUStats is based on telegraf CPUStats.
type CPUStats struct{}

// TOML encodes to toml string
func (c *CPUStats) TOML() string {
	return `[[inputs.cpu]]
`
}
