package influxql

// NotImplementedError is returned when a specific operation is unavailable.
type NotImplementedError struct {
	Op string // Op is the name of the unimplemented operation
}

func (e *NotImplementedError) Error() string {
	return "not implemented: " + e.Op
}

// ErrNotImplemented creates a NotImplementedError specifying op is unavailable.
func ErrNotImplemented(op string) error {
	return &NotImplementedError{Op: op}
}
