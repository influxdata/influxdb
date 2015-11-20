package graphite

import "fmt"

type ErrUnsupportedValue struct {
	Field string
	Value float64
}

func (err *ErrUnsupportedValue) Error() string {
	return fmt.Sprintf(`field "%s" value: "%v" is unsupported`, err.Field, err.Value)
}
