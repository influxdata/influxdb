package errors

// Capture is a wrapper function which can be used to capture errors from closing via a defer.
// An example:
// func Example() (err error) {
//     f, _ := os.Open(...)
//     defer errors.Capture(&err, f.Close)()
//     ...
//     return
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
