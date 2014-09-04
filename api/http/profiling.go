// +build profile

package http

import "net/http/pprof"

func registerProfilingEndpoints(h *HttpServer) {
	// profiling
	h.registerEndpoint("get", "/debug/pprof/cmdline", pprof.Cmdline)
	h.registerEndpoint("get", "/debug/pprof/profile", pprof.Profile)
	h.registerEndpoint("get", "/debug/pprof/symbol", pprof.Symbol)
	h.registerEndpoint("get", "/debug/pprof/", pprof.Index)
	h.registerEndpoint("post", "/debug/pprof/cmdline", pprof.Cmdline)
	h.registerEndpoint("post", "/debug/pprof/profile", pprof.Profile)
	h.registerEndpoint("post", "/debug/pprof/symbol", pprof.Symbol)
	h.registerEndpoint("post", "/debug/pprof/", pprof.Index)
}
