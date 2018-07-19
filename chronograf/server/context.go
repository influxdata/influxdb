package server

import (
	"context"
)

type serverContextKey string

// ServerContextKey is the key used to specify that the
// server is making the requet via context
const ServerContextKey = serverContextKey("server")

// hasServerContext speficies if the context contains
// the ServerContextKey and that the value stored there is true
func hasServerContext(ctx context.Context) bool {
	// prevents panic in case of nil context
	if ctx == nil {
		return false
	}
	sa, ok := ctx.Value(ServerContextKey).(bool)
	// should never happen
	if !ok {
		return false
	}
	return sa
}

func serverContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ServerContextKey, true)
}
