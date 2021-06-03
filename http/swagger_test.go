package http

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestValidSwagger(t *testing.T) {
	t.Skip("swagger.yml no longer being served after migration to openapi repo: https://github.com/influxdata/influxdb/issues/21541")
	data, err := ioutil.ReadFile("./swagger.yml")
	if err != nil {
		t.Fatalf("unable to read swagger specification: %v", err)
	}

	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromData(data)
	if err != nil {
		t.Fatalf("unable to load swagger specification: %v", err)
	}

	if err := swagger.Validate(context.Background()); err != nil {
		t.Errorf("invalid swagger specification: %v", err)
	}
}
