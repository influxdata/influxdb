package mrfusion

// General errors.
const (
	ErrUpstreamTimeout     = Error("request to backend timed out")
	ErrExplorationNotFound = Error("exploration not found")
	ErrSourceNotFound      = Error("source not found")
	ErrServerNotFound      = Error("server not found")
)

// Error is a domain error encountered while processing mrfusion requests
type Error string

func (e Error) Error() string {
	return string(e)
}
