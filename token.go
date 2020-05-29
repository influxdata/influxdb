package influxdb

// TokenGenerator represents a generator for API tokens.
type TokenGenerator interface {
	// Token generates a new API token.
	Token() (string, error)
}
