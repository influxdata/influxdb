package http

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/deepmap/oapi-codegen/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestValidSwagger(t *testing.T) {
	// util.LoadSwagger sets `IsExternalRefsAllowed` to true on the loader.
	// oapi-codegen is being used here because it is what is used to generate
	// the client code despite the same underlying openapi3 library being used.
	swagger, err := util.LoadSwagger("./swagger.yml")
	if err != nil {
		t.Fatalf("unable to load swagger specification: %v", err)
	}

	if err := swagger.Validate(context.Background()); err != nil {
		t.Errorf("invalid swagger specification: %v", err)
	}

	b, err := json.MarshalIndent(swagger, "", "  ")
	require.NoError(t, err)
	ioutil.WriteFile("./swagger.json", b, 0644)
}
