package notification

import (
	"bytes"
	"strconv"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
)

// Duration is a custom type used for generating flux compatible durations.
type Duration ast.DurationLiteral

// TimeDuration convert notification.Duration to time.Duration.
func (d Duration) TimeDuration() time.Duration {
	dl := ast.DurationLiteral(d)
	dd, _ := ast.DurationFrom(&dl, time.Time{})
	return dd
}

// MarshalJSON turns a Duration into a JSON-ified string.
func (d Duration) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('"')
	for _, d := range d.Values {
		b.WriteString(strconv.Itoa(int(d.Magnitude)))
		b.WriteString(d.Unit)
	}
	b.WriteByte('"')

	return b.Bytes(), nil
}

// UnmarshalJSON turns a flux duration literal into a Duration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	dur, err := parser.ParseDuration(string(b[1 : len(b)-1]))
	if err != nil {
		return err
	}

	*d = *(*Duration)(dur)

	return nil
}

// FromTimeDuration converts a time.Duration to a notification.Duration type.
func FromTimeDuration(d time.Duration) (Duration, error) {
	dur, err := parser.ParseDuration(d.String())
	if err != nil {
		return Duration{}, err
	}
	return Duration(*dur), nil
}
