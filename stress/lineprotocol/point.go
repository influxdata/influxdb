package lineprotocol

import (
	"io"
	"time"
)

// Point defines values that will be written in line protocol
type Point interface {
	// byte slice representing the series key for a point
	Series() []byte

	// slice of Field (alais for io.WriterTo) interfaces
	Fields() []Field

	Time() *Timestamp
	SetTime(time.Time)

	// update is intended to be used to mutate field values
	Update()
}

// WritePoint takes in an io.Writer and a Point and writes that point
// to the writer.
func WritePoint(w io.Writer, p Point) (err error) {

	// Write the series key
	_, err = w.Write(p.Series())
	if err != nil {
		return
	}

	// Write Space
	_, err = w.Write([]byte(" "))
	if err != nil {
		return
	}

	// Write each of the fields
	for i, f := range p.Fields() {
		if i != 0 {
			// Add a comma for all but the first field
			_, err = w.Write([]byte(","))
			if err != nil {
				return
			}
		}
		// Write the field to w
		_, err = f.WriteTo(w)
		if err != nil {
			return
		}
	}

	// Write Space
	_, err = w.Write([]byte(" "))
	if err != nil {
		return
	}

	// Write timestamp
	_, err = p.Time().WriteTo(w)
	if err != nil {
		return
	}

	// Write new line
	_, err = w.Write([]byte("\n"))
	if err != nil {
		return
	}

	return
}
