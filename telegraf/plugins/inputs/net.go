package inputs

// NetIOStats is based on telegraf NetIOStats.
type NetIOStats struct{}

// TOML encodes to toml string
func (n *NetIOStats) TOML() string {
	return `[[inputs.net]]
`
}
