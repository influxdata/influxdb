package chronograf

// General errors.
const (
	ErrUpstreamTimeout     = Error("request to backend timed out")
	ErrExplorationNotFound = Error("exploration not found")
	ErrSourceNotFound      = Error("source not found")
	ErrServerNotFound      = Error("server not found")
	ErrLayoutNotFound      = Error("layout not found")
	ErrAuthentication      = Error("user not authenticated")
)

// Error is a domain error encountered while processing chronograf requests
type Error string

func (e Error) Error() string {
	return string(e)
}
