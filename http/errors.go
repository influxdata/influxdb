package http

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/pkg/errors"
)

// AuthzError is returned for authorization errors. When this error type is returned,
// the user can be presented with a generic "authorization failed" error, but
// the system can log the underlying AuthzError() so that operators have insight
// into what actually failed with authorization.
type AuthzError interface {
	error
	AuthzError() error
}

// CheckErrorStatus for status and any error in the response.
func CheckErrorStatus(code int, res *http.Response) error {
	err := CheckError(res)
	if err != nil {
		return err
	}

	if res.StatusCode != code {
		return fmt.Errorf("unexpected status code: %s", res.Status)
	}

	return nil
}

// CheckError reads the http.Response and returns an error if one exists.
// It will automatically recognize the errors returned by Influx services
// and decode the error into an internal error type. If the error cannot
// be determined in that way, it will create a generic error message.
//
// If there is no error, then this returns nil.
func CheckError(resp *http.Response) (err error) {
	switch resp.StatusCode / 100 {
	case 4, 5:
		// We will attempt to parse this error outside of this block.
	case 2:
		return nil
	default:
		// TODO(jsternberg): Figure out what to do here?
		return &platform.Error{
			Code: platform.EInternal,
			Msg:  fmt.Sprintf("unexpected status code: %d %s", resp.StatusCode, resp.Status),
		}
	}

	if resp.StatusCode == http.StatusUnsupportedMediaType {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  fmt.Sprintf("invalid media type: %q", resp.Header.Get("Content-Type")),
		}
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		// Assume JSON if there is no content-type.
		contentType = "application/json"
	}
	mediatype, _, _ := mime.ParseMediaType(contentType)

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return &platform.Error{
			Code: platform.EInternal,
			Msg:  err.Error(),
		}
	}

	switch mediatype {
	case "application/json":
		pe := new(platform.Error)
		if err := json.Unmarshal(buf.Bytes(), pe); err != nil {
			line, _ := buf.ReadString('\n')
			return errors.Wrap(stderrors.New(strings.TrimSuffix(line, "\n")), err.Error())
		}
		return pe
	default:
		line, _ := buf.ReadString('\n')
		return stderrors.New(strings.TrimSuffix(line, "\n"))
	}
}

// UnauthorizedError encodes a error message and status code for unauthorized access.
func UnauthorizedError(ctx context.Context, h platform.HTTPErrorHandler, w http.ResponseWriter) {
	h.HandleHTTPError(ctx, &platform.Error{
		Code: platform.EUnauthorized,
		Msg:  "unauthorized access",
	}, w)
}

// InactiveUserError encode a error message and status code for inactive users.
func InactiveUserError(ctx context.Context, h platform.HTTPErrorHandler, w http.ResponseWriter) {
	h.HandleHTTPError(ctx, &platform.Error{
		Code: platform.EForbidden,
		Msg:  "User is inactive",
	}, w)
}
