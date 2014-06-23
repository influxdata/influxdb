package http

import (
	"compress/gzip"
	"compress/zlib"
	"io"
	libhttp "net/http"
	"strings"
)

type Flusher interface {
	Flush() error
}

type CompressedResponseWriter struct {
	responseWriter     libhttp.ResponseWriter
	writer             io.Writer
	compressionFlusher Flusher
	responseFlusher    libhttp.Flusher
}

func NewCompressionResponseWriter(useCompression bool, rw libhttp.ResponseWriter, req *libhttp.Request) *CompressedResponseWriter {
	responseFlusher, _ := rw.(libhttp.Flusher)

	if req.Header.Get("Accept-Encoding") != "" {
		encodings := strings.Split(req.Header.Get("Accept-Encoding"), ",")

		for _, val := range encodings {
			if val == "gzip" {
				rw.Header().Set("Content-Encoding", "gzip")
				w, _ := gzip.NewWriterLevel(rw, gzip.BestSpeed)
				return &CompressedResponseWriter{rw, w, w, responseFlusher}
			} else if val == "deflate" {
				rw.Header().Set("Content-Encoding", "deflate")
				w, _ := zlib.NewWriterLevel(rw, zlib.BestSpeed)
				return &CompressedResponseWriter{rw, w, w, responseFlusher}
			}
		}
	}

	return &CompressedResponseWriter{rw, rw, nil, responseFlusher}
}

func (self *CompressedResponseWriter) Header() libhttp.Header {
	return self.responseWriter.Header()
}

func (self *CompressedResponseWriter) Write(bs []byte) (int, error) {
	return self.writer.Write(bs)
}

func (self *CompressedResponseWriter) Flush() {
	if self.compressionFlusher != nil {
		self.compressionFlusher.Flush()
	}

	if self.responseFlusher != nil {
		self.responseFlusher.Flush()
	}
}

func (self *CompressedResponseWriter) WriteHeader(responseCode int) {
	self.responseWriter.WriteHeader(responseCode)
}

func CompressionHandler(enableCompression bool, handler libhttp.HandlerFunc) libhttp.HandlerFunc {
	if !enableCompression {
		return handler
	}

	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		crw := NewCompressionResponseWriter(true, rw, req)
		handler(crw, req)
		switch x := crw.writer.(type) {
		case *gzip.Writer:
			x.Close()
		case *zlib.Writer:
			x.Close()
		}
	}
}
