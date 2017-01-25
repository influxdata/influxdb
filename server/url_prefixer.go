package server

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
)

// URLPrefixer is a wrapper for an http.Handler that will prefix all occurrences of a relative URL with the configured Prefix
type URLPrefixer struct {
	Prefix string
	Next   http.Handler
}

type wrapResponseWriter struct {
	http.ResponseWriter
	Substitute *io.PipeWriter
}

func (wrw wrapResponseWriter) Write(p []byte) (int, error) {
	outCount, err := wrw.Substitute.Write(p)
	if err != nil || outCount == len(p) {
		wrw.Substitute.Close()
	}
	return outCount, err
}

// ServeHTTP implements an http.Handler that prefixes relative URLs from the Next handler with the configured prefix
func (up *URLPrefixer) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	nextRead, nextWrite := io.Pipe()
	go up.Next.ServeHTTP(wrapResponseWriter{rw, nextWrite}, r)

	srctag := []byte(`src="`)

	// Locate src tags, flushing everything that isn't to rw
	b := make([]byte, len(srctag))
	io.ReadFull(nextRead, b) // prime the buffer with the start of the input

	buf := bytes.NewBuffer(b)
	src := bufio.NewScanner(nextRead)
	src.Split(bufio.ScanBytes)
	for {
		window := buf.Bytes()
		// advance a byte if window is not a src attr
		if !bytes.Equal(window, srctag) {
			if src.Scan() {
				rw.Write(buf.Next(1))
				buf.Write(src.Bytes())
			} else {
				rw.Write(window)
				break
			}
			continue
		} else {
			buf.Next(len(srctag)) // advance to the relative URL
			for i := 0; i < len(srctag); i++ {
				src.Scan()
				buf.Write(src.Bytes())
			}
			rw.Write(srctag)              // add the src attr to the output
			io.WriteString(rw, up.Prefix) // write the prefix
		}
	}
}
