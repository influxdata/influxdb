package inputs

// Processes is based on telegraf Processes.
type Processes struct{}

// TOML encodes to toml string
func (p *Processes) TOML() string {
	return `[[inputs.processes]]
`
}
