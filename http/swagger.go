package http

// Only generate an asset for swagger.yml.
//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -o swagger_gen.go -tags assets -nocompress -pkg http ./swagger.yml

import (
	"net/http"
	"sync"

	"github.com/ghodss/yaml"
	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
)

var _ http.Handler = (*swaggerLoader)(nil)

// swaggerLoader manages loading the swagger asset and serving it as JSON.
type swaggerLoader struct {
	logger *zap.Logger

	// Ensure we only call initialize once.
	once sync.Once

	// The swagger converted from YAML to JSON.
	json []byte

	// The error loading the swagger asset.
	loadErr error
}

func newSwaggerLoader(logger *zap.Logger) *swaggerLoader {
	return &swaggerLoader{logger: logger}
}

func (s *swaggerLoader) initialize() {
	swagger, err := s.asset(Asset("swagger.yml"))
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
		EncodeError(r.Context(), &influxdb.Error{
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
