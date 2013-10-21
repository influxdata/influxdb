package http

import (
	libhttp "net/http"
)

func CorsHeaderHandler(handler libhttp.HandlerFunc) libhttp.HandlerFunc {
	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		rw.Header().Add("Access-Control-Allow-Origin", "*")
		rw.Header().Add("Access-Control-Max-Age", "2592000")
		rw.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		rw.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		handler(rw, req)
	}
}

func CorsAndCompressionHeaderHandler(handler libhttp.HandlerFunc) libhttp.HandlerFunc {
	return CorsHeaderHandler(CompressionHandler(true, handler))
}
