package storage

import (
	"context"
	"io"
)

// opener is something that can be opened and closed.
type opener interface {
	Open(context.Context) error
	io.Closer // TODO consider a closer-with-context instead
}

// openHelper is a helper to abstract the pattern of opening multiple things,
// exiting early if any open fails, and closing any of the opened things
// in the case of failure.
type openHelper struct {
	opened []io.Closer
	err    error
}

// Open attempts to open the opener. If an error has happened already
// then no calls are made to the opener.
func (o *openHelper) Open(ctx context.Context, op opener) {
	if o.err != nil {
		return
	}
	o.err = op.Open(ctx)
	if o.err == nil {
		o.opened = append(o.opened, op)
	}
}

// Done returns the error of the first open and closes in reverse
// order any opens that have already happened if there was an error.
func (o *openHelper) Done() error {
	if o.err == nil {
		return nil
	}
	for i := len(o.opened) - 1; i >= 0; i-- {
		o.opened[i].Close()
	}
	return o.err
}

// closeHelper is a helper to abstract the pattern of closing multiple
// things and keeping track of the first encountered error.
type closeHelper struct {
	err error
}

// Close closes the closer and keeps track of the first error.
func (c *closeHelper) Close(cl io.Closer) {
	if err := cl.Close(); c.err == nil {
		c.err = err
	}
}

// Done returns the first error.
func (c *closeHelper) Done() error {
	return c.err
}
