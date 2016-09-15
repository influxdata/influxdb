package influx

const (
	ErrTimedOut string = "Timeout during request to InfluxDB"
)

// TimeoutError is returned when requests to InfluxDB are cancelled or time
// out.
type TimeoutError struct{}

func (te TimeoutError) Error() string {
	return ErrTimedOut
}
