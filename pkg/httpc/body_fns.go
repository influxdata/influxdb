package httpc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
)

// BodyFn provides a writer to which a value will be written to
// that will make it's way into the HTTP request.
type BodyFn func(w io.Writer) (header string, headerVal string, err error)

// BodyEmpty returns an empty body.
func BodyEmpty(io.Writer) (string, string, error) {
	return "", "", nil
}

// BodyGob gob encodes the value provided for the HTTP request. Sets the
// Content-Encoding to application/gob.
func BodyGob(v interface{}) BodyFn {
	return func(w io.Writer) (string, string, error) {
		return headerContentEncoding, "application/gob", gob.NewEncoder(w).Encode(v)
	}
}

// BodyJSON JSON encodes the value provided for the HTTP request. Sets the
// Content-Type to application/json.
func BodyJSON(v interface{}) BodyFn {
	return func(w io.Writer) (string, string, error) {
		return headerContentType, "application/json", json.NewEncoder(w).Encode(v)
	}
}

type nopBufCloser struct {
	bytes.Buffer
}

func (*nopBufCloser) Close() error {
	return nil
}
