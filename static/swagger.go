package static

import (
	"net/http"
	"sync"

	"github.com/ghodss/yaml"
	"github.com/influxdata/influxdb/v2"
	"go.uber.org/zap"
)

var _ http.Handler = (*swaggerLoader)(nil)

// swaggerLoader manages loading the swagger asset and serving it as JSON.
type swaggerLoader struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	// Ensure we only call initialize once.
	once sync.Once

	// The swagger converted from YAML to JSON.
	json []byte

	// The error loading the swagger asset.
	loadErr error
}

func newSwaggerLoader(log *zap.Logger, h influxdb.HTTPErrorHandler) *swaggerLoader {
	return &swaggerLoader{
		log:              log,
		HTTPErrorHandler: h,
	}
}

func (s *swaggerLoader) initialize() {
	swagger, err := s.asset(Asset("data/swagger.yml"))
	if err != nil {
		s.loadErr = err
		return
	}

	j, err := yaml.YAMLToJSON(swagger)
	if err == nil {
		s.json = j
	} else {
		s.loadErr = err
	}
}

func (s *swaggerLoader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.once.Do(s.initialize)

	if s.loadErr != nil {
		s.HandleHTTPError(r.Context(), &influxdb.Error{
			Err:  s.loadErr,
			Msg:  "this developer binary not built with assets",
			Code: influxdb.EInternal,
		}, w)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.json)
}
