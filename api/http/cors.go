package http

import (
	libhttp "net/http"
)

func HeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return func(rw libhttp.ResponseWriter, req *libhttp.Request) {
		rw.Header().Add("Access-Control-Allow-Origin", "*")
		rw.Header().Add("Access-Control-Max-Age", "2592000")
		rw.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
		rw.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		rw.Header().Add("X-Influxdb-Version", version)
		handler(rw, req)
	}
}

func CompressionHeaderHandler(handler libhttp.HandlerFunc, version string) libhttp.HandlerFunc {
	return HeaderHandler(CompressionHandler(true, handler), version)
}
