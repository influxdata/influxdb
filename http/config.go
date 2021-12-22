package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const prefixConfig = "/api/v2/config"

func errInvalidType(dest interface{}, flag string) error {
	return &errors.Error{
		Code: errors.EInternal,
		Err:  fmt.Errorf("unknown destination type %T for %q", dest, flag),
	}
}

type parsedOpt map[string]optValue

type optValue []byte

func (o optValue) MarshalJSON() ([]byte, error) { return o, nil }

type ConfigHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	config parsedOpt
}

// NewConfigHandler creates a handler that will return a JSON object with key/value pairs for the configuration values
// used during the launcher startup. The opts slice provides a list of options names along with a pointer to their
// value.
func NewConfigHandler(log *zap.Logger, opts []cli.Opt) (*ConfigHandler, error) {
	h := &ConfigHandler{
		log: log,
		api: kithttp.NewAPI(kithttp.WithLog(log)),
	}

	if err := h.parseOptions(opts); err != nil {
		return nil, err
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
		h.mwAuthorize,
	)

	r.Get("/", h.handleGetConfig)
	h.Router = r
	return h, nil
}

func (h *ConfigHandler) Prefix() string {
	return prefixConfig
}

func (h *ConfigHandler) parseOptions(opts []cli.Opt) error {
	h.config = make(parsedOpt)

	for _, o := range opts {
		var b []byte
		switch o.DestP.(type) {
		// Known types for configuration values. Currently, these can all be encoded directly with json.Marshal.
		case *string, *int, *int32, *int64, *bool, *time.Duration, *[]string, *map[string]string, pflag.Value, *platform.ID, *zapcore.Level:
			var err error
			b, err = json.Marshal(o.DestP)
			if err != nil {
				return err
			}
		default:
			// Return an error if we don't know how to marshal this type.
			return errInvalidType(o.DestP, o.Flag)
		}

		h.config[o.Flag] = b
	}

	return nil
}

func (h *ConfigHandler) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	h.api.Respond(w, r, http.StatusOK, map[string]parsedOpt{"config": h.config})
}

func (h *ConfigHandler) mwAuthorize(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if err := authorizer.IsAllowedAll(r.Context(), influxdb.OperPermissions()); err != nil {
			h.api.Err(w, r, &errors.Error{
				Code: errors.EUnauthorized,
				Msg:  fmt.Sprintf("access to %s requires operator permissions", h.Prefix()),
			})
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
