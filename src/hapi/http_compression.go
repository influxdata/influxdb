package hapi

import (
	"compress/gzip"
	"compress/zlib"
	"io"
	"net/http"
	"strings"
)

type CompressedResponseWriter struct {
	responseWriter http.ResponseWriter
	writer         io.Writer
}

func NewCompressionResponseWriter(useCompression bool, rw http.ResponseWriter, req *http.Request) *CompressedResponseWriter {
	var writer io.Writer = rw

	if req.Header.Get("Accept-Encoding") != "" {
		encodings := strings.Split(req.Header.Get("Accept-Encoding"), ",")

		for _, val := range encodings {
			if val == "gzip" {
				rw.Header().Set("Content-Encoding", "gzip")
				writer, _ = gzip.NewWriterLevel(writer, gzip.BestSpeed)
				break
			} else if val == "deflate" {
				rw.Header().Set("Content-Encoding", "deflate")
				writer, _ = zlib.NewWriterLevel(writer, zlib.BestSpeed)
				break
			}
		}
	}
	return &CompressedResponseWriter{rw, writer}
}

func (self *CompressedResponseWriter) Header() http.Header {
	return self.responseWriter.Header()
}

func (self *CompressedResponseWriter) Write(bs []byte) (int, error) {
	return self.writer.Write(bs)
}

func (self *CompressedResponseWriter) WriteHeader(responseCode int) {
	self.responseWriter.WriteHeader(responseCode)
}

func CompressionHandler(enableCompression bool, handler http.HandlerFunc) http.HandlerFunc {
	if !enableCompression {
		return handler
	}

	return func(rw http.ResponseWriter, req *http.Request) {
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
