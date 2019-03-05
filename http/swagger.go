package http

// Only generate an asset for swagger.yml.
//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -o swagger_gen.go -tags assets -nocompress -pkg http ./swagger.yml

import (
	"context"
	"net/http"

	"github.com/ghodss/yaml"
	"github.com/influxdata/influxdb"
)

// SwaggerHandler servers the swagger.json file from bindata
func SwaggerHandler() http.HandlerFunc {
	swagger, err := Asset("swagger.yml")
	var json []byte
	if err == nil {
		json, err = yaml.YAMLToJSON(swagger)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err != nil {
			EncodeError(context.Background(), &influxdb.Error{
				Err:  err,
				Msg:  "this developer binary not built with assets",
				Code: influxdb.EInternal,
			}, w)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(json)
	})
}
