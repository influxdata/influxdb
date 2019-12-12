package check

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
)

var _ influxdb.Check = &Custom{}

// Custom is the custom check.
type Custom struct {
	Base
	TimeSince *notification.Duration `json:"timeSince,omitempty"`
	StaleTime *notification.Duration `json:"staleTime,omitempty"`
	// If only zero values reported since time, trigger alert.
	// TODO(desa): Is this implemented in Flux?
	ReportZero bool                    `json:"reportZero"`
	Level      notification.CheckLevel `json:"level"`
}

// Type returns the type of the check.
func (c Custom) Type() string {
	return "custom"
}

// GenerateFlux returns a flux script for the Custom provided.
func (c Custom) GenerateFlux() (string, error) {
	p, err := c.GenerateFluxAST()
	if err != nil {
		return "", &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  err.Error(),
		}
	}

	return ast.Format(p), nil
}

// GenerateFluxAST returns a flux AST for the custom provided. If there
// are any errors in the flux that the user provided the function will return
// an error for each error found when the script is parsed.
func (c Custom) GenerateFluxAST() (*ast.Package, error) {
	p := parser.ParseSource(c.Query.Text)

	if errs := ast.GetErrors(p); len(errs) != 0 {
		return nil, multiError(errs)
	}

	if len(p.Files) != 1 {
		return nil, fmt.Errorf("expect a single file to be returned from query parsing got %d", len(p.Files))
	}

	return p, nil
}

type customAlias Custom

// MarshalJSON implement json.Marshaler interface.
func (c Custom) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			customAlias
			Type string `json:"type"`
		}{
			customAlias: customAlias(c),
			Type:        c.Type(),
		})
}
