package http

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func TestValidSwagger(t *testing.T) {
	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromFile("./swagger.yml")
	if err != nil {
		t.Fatalf("unable to load swagger specification: %v", err)
	}

	if err := swagger.Validate(context.Background()); err != nil {
		t.Errorf("invalid swagger specification: %v", err)
	}
}
