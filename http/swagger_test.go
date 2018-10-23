package http

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestValidSwagger(t *testing.T) {
	data, err := ioutil.ReadFile("./swagger.yml")
	if err != nil {
		t.Fatalf("unable to read swagger specification: %v", err)
	}
	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromYAMLData(data)
	if err != nil {
		t.Fatalf("unable to load swagger specification: %v", err)
	}
	if err := swagger.Validate(context.Background()); err != nil {
		t.Errorf("invalid swagger specification: %v", err)
	}
}
