// +build !debug_ref

package lifecycle

import "errors"

// When not in debug mode, all of the expensive tracking is compile time
// disabled so that it has no overhead.

var errResourceClosed = errors.New("resource closed")

func resourceClosed() error { return errResourceClosed }

type emptyLive struct{}

var live emptyLive

func (emptyLive) track(r *Reference) *Reference { return r }

func (emptyLive) untrack(r *Reference) {}
