package api

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-stack/stack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	influxlogger "github.com/influxdata/influxdb/logger"
	influxdb "github.com/influxdata/influxdb/servicesv2"
	kithttp "github.com/influxdata/influxdb/servicesv2/kit/http"
)

// ApiHandler a modular generic api handling mechanizm
type APIHandler struct {
	bindAddr string
	api      *kithttp.API

	httpServer *http.Server
	chi.Router
}

// NewAPIHandler constructs all api handlers beneath it and returns an APIHandler
func NewAPIHandler(bindAddr string, middlewares ...kithttp.Middleware) *APIHandler {
	api := kithttp.NewAPI()
	h := &APIHandler{
		bindAddr: bindAddr,
		api:      api,
		Router:   NewBaseChiRouter(api),
	}

	for _, middleware := range middlewares {
		h.Use(middleware)
	}
	return h
}

// WithResourceHandler registers a resource handler on the APIHandler.
func (h *APIHandler) WithResourceHandler(resHandler kithttp.ResourceHandler) {
	h.Mount(resHandler.Prefix(), resHandler)
}

func (h *APIHandler) WithLogger(log *zap.Logger) {
	h.api.WithLogger(log)
}

func (h *APIHandler) Open() error {
	// generate a listener ??? port
	ln, err := net.Listen("tcp", h.bindAddr)
	if err != nil {
		return err
	}

	// assign listener to chi
	h.httpServer = &http.Server{
		Addr:    h.bindAddr,
		Handler: h.Router,
	}

	return h.httpServer.Serve(ln)
}

func (h *APIHandler) Close() error {
	return h.httpServer.Close()
}

// NewBaseChiRouter returns a new chi router with a 404 handler, a 405 handler, and a panic handler.
func NewBaseChiRouter(api *kithttp.API) chi.Router {
	router := chi.NewRouter()
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		api.Err(w, r, &influxdb.Error{
			Code: influxdb.ENotFound,
			Msg:  "path not found",
		})
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		api.Err(w, r, &influxdb.Error{
			Code: influxdb.EMethodNotAllowed,
			Msg:  fmt.Sprintf("allow: %s", w.Header().Get("Allow")),
		})
	})
	router.Use(
		panicMW(api),
		kithttp.SkipOptions,
		middleware.StripSlashes,
		kithttp.SetCORS,
	)
	return router
}

func panicMW(api *kithttp.API) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				panicErr := recover()
				if panicErr == nil {
					return
				}

				pe := &influxdb.Error{
					Code: influxdb.EInternal,
					Msg:  "a panic has occurred",
					Err:  fmt.Errorf("%s: %v", r.URL.String(), panicErr),
				}

				l := getPanicLogger()
				if entry := l.Check(zapcore.ErrorLevel, pe.Msg); entry != nil {
					entry.Stack = fmt.Sprintf("%+v", stack.Trace())
					entry.Write(zap.Error(pe.Err))
				}

				api.Err(w, r, pe)
			}()
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
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
