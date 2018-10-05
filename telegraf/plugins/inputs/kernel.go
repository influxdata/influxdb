package inputs

// Kernel is based on telegraf Kernel.
type Kernel struct{}

// TOML encodes to toml string
func (k *Kernel) TOML() string {
	return `[[inputs.kernel]]
`
}
