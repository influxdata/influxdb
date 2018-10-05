package inputs

// NetResponse is based on telegraf NetResponse.
type NetResponse struct{}

// TOML encodes to toml string
func (n *NetResponse) TOML() string {
	return `[[inputs.net_response]]
`
}
