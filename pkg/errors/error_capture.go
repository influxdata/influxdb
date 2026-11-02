package errors

import (
	stderrors "errors"
	"fmt"
)

// Capture is a wrapper function which can be used to capture errors from closing via a defer.
// An example:
//
//	func Example() (err error) {
//	    f, _ := os.Open(...)
//	    defer errors.Capture(&err, f.Close)()
//	    ...
//	    return
//
// Doing this will result in the error from the f.Close() call being
// put in the error via a ptr, if the error is not nil
func Capture(rErr *error, fn func() error) func() {
	return func() {
		err := fn()
		if *rErr == nil {
			*rErr = err
		}
	}
}

// Capturef is similar to Capture but allows adding context when an error occurs.
// If an fn returns an error, then a new error is creating by concatenating ": %w"
// to fs and adding the error to the end of a.
//
// An additional difference to Capture is that if fn() returns an error, the
// new error is joined to the existing rErr using errors.Join.
//
//	func Example(fn string) (rErr error) {
//	    f, err := os.Open(fn)
//	    if err != nil {
//	        return err
//	    }
//	    defer errors.Capturef(&rErr, f.Close, "error closing %q", fn)()
//	    ....
//	}
//
// As an illustration, if Example is called with Example("meta.boltdb") and f.Close
// fails with a disk full error (syscall.ENOSPC), then the error string returned
// would be `error closing "meta.boltdb": no space left on device` and
// errors.Is(Example("meta.boltdb"), syscall.ENOSPC) would return true.
//
// If fs is empty, a is ignored and the error returned by fn() is appended
// to rErr unmodified.
func Capturef(rErr *error, fn func() error, fs string, a ...any) func() {
	return func() {
		if fn != nil {
			if err := fn(); err != nil {
				if rErr != nil {
					// Eventually, the behavior regarding an empty fs will allow
					// Capture to be rewritten as a simple wrapper around Capturef.
					// This can happen once we decide Capture should use errors.Join.
					if fs != "" {
						args := make([]any, 0, len(a)+1)
						args = append(args, a...)
						args = append(args, err)
						err = fmt.Errorf(fs+": %w", args...)
					}
					*rErr = stderrors.Join(*rErr, err)
				}
			}
		}
	}
}
