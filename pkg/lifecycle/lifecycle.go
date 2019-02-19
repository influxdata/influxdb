package lifecycle

import (
	"io"
)

// Resource is something that can be opened and closed.
type Resource interface {
	Open() error
	io.Closer
}

// Opener is a helper to abstract the pattern of opening multiple things,
// exiting early if any open fails, and closing any of the opened things
// in the case of failure.
type Opener struct {
	opened []io.Closer
	err    error
}

// Open attempts to open the resource. If an error has happened already
// then no calls are made to the resource.
func (o *Opener) Open(res Resource) {
	if o.err != nil {
		return
	}
	o.err = res.Open()
	if o.err == nil {
		o.opened = append(o.opened, res)
	}
}

// Done returns the error of the first open and closes in reverse
// order any opens that have already happened if there was an error.
func (o *Opener) Done() error {
	if o.err == nil {
		return nil
	}
	for i := len(o.opened) - 1; i >= 0; i-- {
		o.opened[i].Close()
	}
	return o.err
}

// Closer is a helper to abstract the pattern of closing multiple
// things and keeping track of the first encountered error.
type Closer struct {
	err error
}

// Close closes the closer and keeps track of the first error.
func (c *Closer) Close(cl io.Closer) {
	if err := cl.Close(); c.err == nil {
		c.err = err
	}
}

// Done returns the first error.
func (c *Closer) Done() error {
	return c.err
}
