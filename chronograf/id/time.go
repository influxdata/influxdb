package id

import (
	"strconv"
	"time"

	"github.com/influxdata/platform/chronograf"
)

// tm generates an id based on current time
type tm struct {
	Now func() time.Time
}

// NewTime builds a chronograf.ID generator based on current time
func NewTime() chronograf.ID {
	return &tm{
		Now: time.Now,
	}
}

// Generate creates a string based on the current time as an integer
func (i *tm) Generate() (string, error) {
	return strconv.Itoa(int(i.Now().Unix())), nil
}
