package inputs

// SwapStats is based on telegraf SwapStats.
type SwapStats struct{}

// TOML encodes to toml string
func (c *SwapStats) TOML() string {
	return `[[inputs.swap]]
`
}
