package http

import (
	"fmt"
	"net/http"
	"os"
	"runtime/debug"
	"sync"

	platform "github.com/influxdata/influxdb"
	influxlogger "github.com/influxdata/influxdb/logger"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NewRouter returns a new router with a 404 handler, a 405 handler, and a panic handler.
func NewRouter() *httprouter.Router {
	router := httprouter.New()
	router.NotFound = http.HandlerFunc(notFoundHandler)
	router.MethodNotAllowed = http.HandlerFunc(methodNotAllowedHandler)
	router.PanicHandler = panicHandler
	return router
}

// notFoundHandler represents a 404 handler that return a JSON response.
func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pe := &platform.Error{
		Code: platform.ENotFound,
		Msg:  "path not found",
	}

	EncodeError(ctx, pe, w)
}

// methodNotAllowedHandler represents a 405 handler that return a JSON response.
func methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	allow := w.Header().Get("Allow")
	pe := &platform.Error{
		Code: platform.EMethodNotAllowed,
		Msg:  fmt.Sprintf("allow: %s", allow),
	}

	EncodeError(ctx, pe, w)
}

// panicHandler handles panics recovered from http handlers.
// It returns a json response with http status code 500 and the recovered error message.
func panicHandler(w http.ResponseWriter, r *http.Request, rcv interface{}) {
	ctx := r.Context()
	pe := &platform.Error{
		Code: platform.EInternal,
		Msg:  "a panic has occurred",
		Err:  fmt.Errorf("%v", rcv),
	}

	l := getPanicLogger()
	l.Error(
		pe.Msg,
		zap.String("err", pe.Err.Error()),
		zap.String("stack", fmt.Sprintf("%s", debug.Stack())),
	)

	EncodeError(ctx, pe, w)
}

var panicLogger *zap.Logger
var panicLoggerOnce sync.Once

// getPanicLogger returns a logger for panicHandler.
func getPanicLogger() *zap.Logger {
	panicLoggerOnce.Do(func() {
		panicLogger = influxlogger.New(os.Stderr)
		panicLogger = panicLogger.With(zap.String("handler", "panic"))
	})

	return panicLogger
}
