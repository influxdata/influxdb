package httpd

import (
	"fmt"
	"net"
	"net/http"
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
		// The status will be StatusOK if WriteHeader has not been called yet
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
// 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
// A "-" in a field indicates missing data.[citation needed]

// 127.0.0.1 is the IP address of the client (remote host) which made the request to the server.
// user-identifier is the RFC 1413 identity of the client.
// frank is the userid of the person requesting the document.
// [10/Oct/2000:13:55:36 -0700] is the date, time, and time zone when the server finished processing the request, by default in strftime format %d/%b/%Y:%H:%M:%S %z.
// "GET /apache_pb.gif HTTP/1.0" is the request line from the client. The method GET, /apache_pb.gif the resource requested, and HTTP/1.0 the HTTP protocol.
// 200 is the HTTP status code returned to the client. 2xx is a successful response, 3xx a redirection, 4xx a client error, and 5xx a server error.
// 2326 is the size of the object returned to the client, measured in bytes.

func buildLogLine(l *responseLogger, r *http.Request, start time.Time) string {
	username := "-"
	url := r.URL
	if url.User != nil {
		if name := url.User.Username(); name != "" {
			username = name
		}
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)

	if err != nil {
		host = r.RemoteAddr
	}

	uri := url.RequestURI()
	return fmt.Sprintf(
		"%s %s %s %s %s %s %s %d %d %s %s",
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
	)
}
