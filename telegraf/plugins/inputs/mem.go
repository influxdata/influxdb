package inputs

// MemStats is based on telegraf MemStats.
type MemStats struct{}

// TOML encodes to toml string
func (m *MemStats) TOML() string {
	return `[[inputs.mem]]
`
}
