package http

import (
	"net/http"
	"strconv"

	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/pkg/errors"
)

// CheckError reads the http.Response and returns an error if one exists.
// It will automatically recognize the errors returned by Influx services
// and decode the error into an internal error type. If the error cannot
// be determined in that way, it will create a generic error message.
//
// If there is no error, then this returns nil.
func CheckError(resp *http.Response) error {
	switch resp.StatusCode / 100 {
	case 4, 5:
		// We will attempt to parse this error outside of this block.
	case 2:
		return nil
	default:
		// TODO(jsternberg): Figure out what to do here?
		return kerrors.InternalErrorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}

	// Attempt to read the X-Influx-Error header with the message.
	if errMsg := resp.Header.Get("X-Influx-Error"); errMsg != "" {
		// Parse the reference number as an integer. If we cannot parse it,
		// return the error message by itself.
		ref, err := strconv.Atoi(resp.Header.Get("X-Influx-Reference"))
		if err != nil {
			// We cannot parse the reference number so just use internal.
			ref = kerrors.InternalError
		}
		return &kerrors.Error{
			Reference: ref,
			Code:      resp.StatusCode,
			Err:       errMsg,
		}
	}

	// There is no influx error so we need to report that we have some kind
	// of error from somewhere.
	// TODO(jsternberg): Try to make this more advance by reading the response
	// and either decoding a possible json message or just reading the text itself.
	// This might be good enough though.
	msg := "unknown server error"
	if resp.StatusCode/100 == 4 {
		msg = "client error"
	}
	return errors.Wrap(errors.New(resp.Status), msg)
}
