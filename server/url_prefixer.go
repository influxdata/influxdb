package server

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net/http"
)

// URLPrefixer is a wrapper for an http.Handler that will prefix all occurrences of a relative URL with the configured Prefix
type URLPrefixer struct {
	Prefix string       // the prefix to be appended after any detected Attrs
	Next   http.Handler // the http.Handler which will generate the content to be modified by this handler
	Attrs  [][]byte     // a list of attrs that should have their URLs prefixed. For example `src="` or `href="` would be valid
}

type wrapResponseWriter struct {
	http.ResponseWriter
	Substitute *io.PipeWriter

	headerWritten bool
	dupHeader     http.Header
}

func (wrw wrapResponseWriter) Write(p []byte) (int, error) {
	return wrw.Substitute.Write(p)
}

func (wrw wrapResponseWriter) WriteHeader(code int) {
	if !wrw.headerWritten {
		wrw.ResponseWriter.Header().Set("Content-Type", wrw.Header().Get("Content-Type"))
		wrw.headerWritten = true
	}
	wrw.ResponseWriter.WriteHeader(code)
}

// Header() copies the Header map from the underlying ResponseWriter to prevent
// modifications to it by callers
func (wrw wrapResponseWriter) Header() http.Header {
	wrw.dupHeader = http.Header{}
	origHeader := wrw.ResponseWriter.Header()
	for k, v := range origHeader {
		wrw.dupHeader[k] = v
	}
	return wrw.dupHeader
}

const CHUNK_SIZE int = 512

// ServeHTTP implements an http.Handler that prefixes relative URLs from the
// Next handler with the configured prefix. It does this by examining the
// stream through the ResponseWriter, and appending the Prefix after any of the
// Attrs detected in the stream.
func (up *URLPrefixer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {

	// chunked transfer because we're modifying the response on the fly, so we
	// won't know the final content-length
	rw.Header().Set("Connection", "Keep-Alive")
	rw.Header().Set("Transfer-Encoding", "chunked")
	//rw.Header().Set("X-Content-Type-Options", "nosniff")

	writtenCount := 0 // number of bytes written to rw

	// extract the flusher for flushing chunks
	flusher, ok := rw.(http.Flusher)
	if !ok {
		log.Fatalln("Exected http.ResponseWriter to be an http.Flusher, but wasn't")
	}

	nextRead, nextWrite := io.Pipe()
	go func() {
		defer nextWrite.Close()
		up.Next.ServeHTTP(wrapResponseWriter{ResponseWriter: rw, Substitute: nextWrite}, r)
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

				if writtenCount >= CHUNK_SIZE {
					flusher.Flush()
					writtenCount = 0
				}
			} else {
				if err := src.Err(); err != nil {
					log.Println("Error encountered while scanning: err:", err)
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
