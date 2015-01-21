package httpd

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type loggingResponseWriter interface {
	http.ResponseWriter
	Status() int
	Size() int
}

// responseLogger is wrapper of http.ResponseWriter that keeps track of its HTTP status
// code and body size
type responseLogger struct {
	w      http.ResponseWriter
	status int
	size   int
}

func (l *responseLogger) Header() http.Header {
	return l.w.Header()
}

func (l *responseLogger) Write(b []byte) (int, error) {
	if l.status == 0 {
		// Set status if WriteHeader has not been called
		l.status = http.StatusOK
	}
	size, err := l.w.Write(b)
	l.size += size
	return size, err
}

func (l *responseLogger) WriteHeader(s int) {
	l.w.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) Status() int {
	return l.status
}

func (l *responseLogger) Size() int {
	return l.size
}

// Common Log Format: http://en.wikipedia.org/wiki/Common_Log_Format

// buildLogLine creates a common log format
// in addittion to the common fields, we also append referrer, user agent and request ID
func buildLogLine(l *responseLogger, r *http.Request, start time.Time) string {
	username := "-"
	url := r.URL

	// get username from the url if passed there
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	// Try to get it from the authorization header if set there
	if username == "-" {
		auth := r.Header.Get("Authorization")
		fields := strings.Split(auth, " ")
		if len(fields) == 2 {
			bs, err := base64.StdEncoding.DecodeString(fields[1])
			if err == nil {
				fields = strings.Split(string(bs), ":")
				if len(fields) >= 1 {
					username = fields[0]
				}
			}
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)

	if err != nil {
		host = r.RemoteAddr
	}

	uri := url.RequestURI()
	return fmt.Sprintf(
		"%s %s %s %s %s %s %s %d %d %s %s %s",
		host,
		"-",
		username,
		fmt.Sprintf("[%s]", start.Format("02/Jan/2006:15:04:05 -0700")),
		r.Method,
		uri,
		r.Proto,
		l.Status(),
		l.Size(),
		r.Referer(),
		r.UserAgent(),
		r.Header.Get("Request-Id"),
	)
}
