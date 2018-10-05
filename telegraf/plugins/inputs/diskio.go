package inputs

// DiskIO is based on telegraf DiskIO.
type DiskIO struct{}

// TOML encodes to toml string
func (d *DiskIO) TOML() string {
	return `[[inputs.diskio]]
`
}
