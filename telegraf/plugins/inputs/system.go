package inputs

// SystemStats is based on telegraf SystemStats.
type SystemStats struct{}

// TOML encodes to toml string
func (s *SystemStats) TOML() string {
	return `[[inputs.system]]
`
}
