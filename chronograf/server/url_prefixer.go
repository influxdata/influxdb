package server

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"regexp"

	"github.com/influxdata/influxdb/chronograf"
)

const (
	ErrNotFlusher = "Expected http.ResponseWriter to be an http.Flusher, but wasn't"
)

// URLPrefixer is a wrapper for an http.Handler that will prefix all occurrences of a relative URL with the configured Prefix
type URLPrefixer struct {
	Prefix string            // the prefix to be appended after any detected Attrs
	Next   http.Handler      // the http.Handler which will generate the content to be modified by this handler
	Attrs  [][]byte          // a list of attrs that should have their URLs prefixed. For example `src="` or `href="` would be valid
	Logger chronograf.Logger // The logger where prefixing errors will be dispatched to
}

type wrapResponseWriter struct {
	http.ResponseWriter
	Substitute *io.PipeWriter

	headerWritten bool
	dupHeader     *http.Header
}

func (wrw *wrapResponseWriter) Write(p []byte) (int, error) {
	return wrw.Substitute.Write(p)
}

func (wrw *wrapResponseWriter) WriteHeader(code int) {
	if !wrw.headerWritten {
		wrw.ResponseWriter.Header().Set("Content-Type", wrw.dupHeader.Get("Content-Type"))
		header := wrw.ResponseWriter.Header()
		// Filter out content length header to prevent stopping writing
		if wrw.dupHeader != nil {
			for k, v := range *wrw.dupHeader {
				if k == "Content-Length" {
					continue
				}
				header[k] = v
			}
		}

		wrw.headerWritten = true
	}
	wrw.ResponseWriter.WriteHeader(code)
}

// Header() copies the Header map from the underlying ResponseWriter to prevent
// modifications to it by callers
func (wrw *wrapResponseWriter) Header() http.Header {
	if wrw.dupHeader == nil {
		h := http.Header{}
		origHeader := wrw.ResponseWriter.Header()
		for k, v := range origHeader {
			h[k] = v
		}
		wrw.dupHeader = &h
	}
	return *wrw.dupHeader
}

// ChunkSize is the number of bytes per chunked transfer-encoding
const ChunkSize int = 512

// ServeHTTP implements an http.Handler that prefixes relative URLs from the
// Next handler with the configured prefix. It does this by examining the
// stream through the ResponseWriter, and appending the Prefix after any of the
// Attrs detected in the stream.
func (up *URLPrefixer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// extract the flusher for flushing chunks
	flusher, ok := rw.(http.Flusher)

	if !ok {
		up.Logger.Info(ErrNotFlusher)
		up.Next.ServeHTTP(rw, r)
		return
	}

	isSVG, _ := regexp.Match(".svg$", []byte(r.URL.String()))
	if isSVG {
		up.Next.ServeHTTP(rw, r)
		return
	}

	// chunked transfer because we're modifying the response on the fly, so we
	// won't know the final content-length
	rw.Header().Set("Connection", "Keep-Alive")
	rw.Header().Set("Transfer-Encoding", "chunked")

	writtenCount := 0 // number of bytes written to rw
	nextRead, nextWrite := io.Pipe()
	go func() {
		defer nextWrite.Close()
		up.Next.ServeHTTP(&wrapResponseWriter{ResponseWriter: rw, Substitute: nextWrite}, r)
	}()

	// setup a buffer which is the max length of our target attrs
	b := make([]byte, up.maxlen(up.Attrs...))
	io.ReadFull(nextRead, b) // prime the buffer with the start of the input
	buf := bytes.NewBuffer(b)

	// Read next handler's response byte by byte
	src := bufio.NewScanner(nextRead)
	src.Split(bufio.ScanBytes)
	for {
		window := buf.Bytes()

		// advance a byte if window is not a src attr
		if matchlen, match := up.match(window, up.Attrs...); matchlen == 0 {
			if src.Scan() {
				// shift the next byte into buf
				rw.Write(buf.Next(1))
				writtenCount++
				buf.Write(src.Bytes())

				if writtenCount >= ChunkSize {
					flusher.Flush()
					writtenCount = 0
				}
			} else {
				if err := src.Err(); err != nil {
					up.Logger.
						WithField("component", "prefixer").
						Error("Error encountered while scanning: err:", err)
				}
				rw.Write(window)
				flusher.Flush()
				break
			}
			continue
		} else {
			buf.Next(matchlen) // advance to the relative URL
			for i := 0; i < matchlen; i++ {
				src.Scan()
				buf.Write(src.Bytes())
			}
			rw.Write(match)               // add the src attr to the output
			io.WriteString(rw, up.Prefix) // write the prefix
		}
	}
}

// match compares the subject against a list of targets. If there is a match
// between any of them a non-zero value is returned. The returned value is the
// length of the match. It is assumed that subject's length > length of all
// targets. The matching []byte is also returned as the second return parameter
func (up *URLPrefixer) match(subject []byte, targets ...[]byte) (int, []byte) {
	for _, target := range targets {
		if bytes.Equal(subject[:len(target)], target) {
			return len(target), target
		}
	}
	return 0, []byte{}
}

// maxlen returns the length of the largest []byte provided to it as an argument
func (up *URLPrefixer) maxlen(targets ...[]byte) int {
	max := 0
	for _, tgt := range targets {
		if tlen := len(tgt); tlen > max {
			max = tlen
		}
	}
	return max
}

// NewDefaultURLPrefixer returns a URLPrefixer that will prefix any src and
// href attributes found in HTML as well as any url() directives found in CSS
// with the provided prefix. Additionally, it will prefix any `data-basepath`
// attributes as well for informing front end logic about any prefixes. `next`
// is the next http.Handler that will have its output prefixed
func NewDefaultURLPrefixer(prefix string, next http.Handler, lg chronograf.Logger) *URLPrefixer {
	return &URLPrefixer{
		Prefix: prefix,
		Next:   next,
		Logger: lg,
		Attrs: [][]byte{
			[]byte(`src="`),
			[]byte(`href="`),
			[]byte(`url(`),
			[]byte(`data-basepath="`), // for forwarding basepath to frontend
		},
	}
}
