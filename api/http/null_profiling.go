// +build !profile

package http

func registerProfilingEndpoints(h *HttpServer) {
	// no profiling if the profile build tag isn't enabled
}
