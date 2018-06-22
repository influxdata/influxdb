package errors

import (
	"context"
	"fmt"
	"net/http"
)

const errorHeaderMaxLength = 64

// TODO: move to http directory

// EncodeHTTP encodes err with the appropriate status code and format,
// sets the X-Influx-Error and X-Influx-Reference headers on the response,
// and sets the response status to the corresponding status code.
func EncodeHTTP(ctx context.Context, err error, w http.ResponseWriter) {
	if err == nil {
		return
	}
	e, ok := err.(*Error)
	if !ok {
		errStr := err.Error()
		if len(errStr) > errorHeaderMaxLength {
			errStr = err.Error()[0:errorHeaderMaxLength]
		}

		e = &Error{
			Reference: InternalError,
			Err:       errStr,
		}
	}
	e.SetCode()

	w.Header().Set("X-Influx-Error", e.Err)
	w.Header().Set("X-Influx-Reference", fmt.Sprintf("%d", e.Reference))
	w.WriteHeader(e.Code)
}
