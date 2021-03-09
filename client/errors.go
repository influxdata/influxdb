package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
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
		return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
	}

	if resp.StatusCode == http.StatusUnsupportedMediaType {
		return fmt.Errorf("invalid media type (%d): %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		// Assume JSON if there is no content-type.
		contentType = "application/json"
	}
	mediatype, _, _ := mime.ParseMediaType(contentType)

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return fmt.Errorf("failed to read error response: %w", err)
	}

	errResponse := Response{}

	switch mediatype {
	case "application/json":
		if err := json.Unmarshal(buf.Bytes(), &errResponse); err != nil {
			return fmt.Errorf("Attempted to unmarshal error (%d) but failed: %w", resp.StatusCode, err)
		}
		if errResponse.Err == nil {
			return fmt.Errorf("Missing error in http response (%d)", resp.StatusCode)
		}
		return fmt.Errorf("Error (%d): %w", resp.StatusCode, errResponse.Err)
	default:
	}
	// Return the first line as the error
	line, _ := buf.ReadString('\n')
	return errors.New(strings.TrimSuffix(line, "\n"))
}
