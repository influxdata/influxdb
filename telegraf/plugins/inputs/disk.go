package inputs

// DiskStats is based on telegraf DiskStats.
type DiskStats struct{}

// TOML encodes to toml string
func (d *DiskStats) TOML() string {
	return `[[inputs.disk]]
`
}
