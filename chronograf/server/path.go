package server

import "net/url"

// PathEscape escapes the string so it can be safely placed inside a URL path segment.
// Change to url.PathEscape for go 1.8
func PathEscape(str string) string {
	u := &url.URL{Path: str}
	return u.String()
}
