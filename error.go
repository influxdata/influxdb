package influxdb

// ChronografError is a domain error encountered while processing chronograf requests.
type ChronografError string

// ChronografError returns the string of an error.
func (e ChronografError) Error() string {
	return string(e)
}
