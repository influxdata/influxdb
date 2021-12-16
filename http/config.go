package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const prefixConfig = "/api/v2/config"

type parsedOpt struct {
	Option string   `json:"option"`
	Value  optValue `json:"value"`
}

type optValue []byte

func (o optValue) MarshalJSON() ([]byte, error) { return o, nil }

type ConfigHandler struct {
	chi.Router

	log *zap.Logger
	api *kithttp.API

	config []parsedOpt
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
		h.mwRequireOperPermissions,
	)

	r.Get("/", h.handleGetConfig)
	h.Router = r
	return h, nil
}

func (h *ConfigHandler) Prefix() string {
	return prefixConfig
}

func (h *ConfigHandler) parseOptions(opts []cli.Opt) error {
	config := make([]parsedOpt, len(opts))

	// Ensure that there are no errors encountered while obtaining the JSON encoding of the config values obtained from
	// the destination pointers. If the value can be successfully encoded, its bytes will be stored in optValue for future
	// marshalling calls.
	for i, o := range opts {
		b, err := json.Marshal(o.DestP)
		if err != nil {
			return err
		}

		config[i] = parsedOpt{
			Option: o.Flag,
			Value:  b,
		}
	}

	h.config = config
	return nil
}

func (h *ConfigHandler) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	h.api.Respond(w, r, http.StatusOK, h.config)
}

func (h *ConfigHandler) mwRequireOperPermissions(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if err := authorizer.IsAllowedAll(ctx, influxdb.OperPermissions()); err != nil {
			h.api.Err(w, r, &errors.Error{
				Code: errors.EUnauthorized,
				Msg:  "access to /config requires operator permissions",
			})
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}
