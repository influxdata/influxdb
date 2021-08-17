package check

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/flux"
	"github.com/influxdata/influxdb/v2/query"
)

var _ influxdb.Check = (*Deadman)(nil)

// Deadman is the deadman check.
type Deadman struct {
	Base
	TimeSince *notification.Duration `json:"timeSince,omitempty"`
	StaleTime *notification.Duration `json:"staleTime,omitempty"`
	// If only zero values reported since time, trigger alert.
	// TODO(desa): Is this implemented in Flux?
	ReportZero bool                    `json:"reportZero"`
	Level      notification.CheckLevel `json:"level"`
}

// Type returns the type of the check.
func (c Deadman) Type() string {
	return "deadman"
}

// GenerateFlux returns a flux script for the Deadman provided.
func (c Deadman) GenerateFlux(lang influxdb.FluxLanguageService) (string, error) {
	f, err := c.GenerateFluxAST(lang)
	if err != nil {
		return "", err
	}

	return astutil.Format(f)
}

// GenerateFluxAST returns a flux AST for the deadman provided. If there
// are any errors in the flux that the user provided the function will return
// an error for each error found when the script is parsed.
func (c Deadman) GenerateFluxAST(lang influxdb.FluxLanguageService) (*ast.File, error) {
	p, err := query.Parse(lang, c.Query.Text)
	if p == nil {
		return nil, err
	}
	removeAggregateWindow(p)
	replaceDurationsWithEvery(p, c.StaleTime)
	removeStopFromRange(p)

	if errs := ast.GetErrors(p); len(errs) != 0 {
		return nil, multiError(errs)
	}

	// TODO(desa): this is a hack that we had to do as a result of https://github.com/influxdata/flux/issues/1701
	// when it is fixed we should use a separate file and not manipulate the existing one.
	if len(p.Files) != 1 {
		return nil, fmt.Errorf("expect a single file to be returned from query parsing got %d", len(p.Files))
	}

	f := p.Files[0]
	assignPipelineToData(f)

	f.Imports = append(f.Imports, flux.Imports("influxdata/influxdb/monitor", "experimental", "influxdata/influxdb/v1")...)
	f.Body = append(f.Body, c.generateFluxASTBody()...)

	return f, nil
}

func (c Deadman) generateFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, c.generateTaskOption())
	statements = append(statements, c.generateFluxASTCheckDefinition("deadman"))
	statements = append(statements, c.generateLevelFn())
	statements = append(statements, c.generateFluxASTMessageFunction())
	return append(statements, c.generateFluxASTChecksFunction())
}

func (c Deadman) generateLevelFn() ast.Statement {
	fn := flux.Function(flux.FunctionParams("r"), flux.Member("r", "dead"))

	lvl := strings.ToLower(c.Level.String())

	return flux.DefineVariable(lvl, fn)
}

func (c Deadman) generateFluxASTChecksFunction() ast.Statement {
	dur := (*ast.DurationLiteral)(c.TimeSince)
	now := flux.Call(flux.Identifier("now"), flux.Object())
	sub := flux.Call(flux.Member("experimental", "subDuration"), flux.Object(flux.Property("from", now), flux.Property("d", dur)))
	return flux.ExpressionStatement(flux.Pipe(
		flux.Identifier("data"),
		flux.Call(flux.Member("v1", "fieldsAsCols"), flux.Object()),
		flux.Call(flux.Member("monitor", "deadman"), flux.Object(flux.Property("t", sub))),
		c.generateFluxASTChecksCall(),
	))
}

func (c Deadman) generateFluxASTChecksCall() *ast.CallExpression {
	objectProps := append(([]*ast.Property)(nil), flux.Property("data", flux.Identifier("check")))
	objectProps = append(objectProps, flux.Property("messageFn", flux.Identifier("messageFn")))

	// This assumes that the ThresholdConfigs we've been provided do not have duplicates.
	lvl := strings.ToLower(c.Level.String())
	objectProps = append(objectProps, flux.Property(lvl, flux.Identifier(lvl)))

	return flux.Call(flux.Member("monitor", "check"), flux.Object(objectProps...))
}

type deadmanAlias Deadman

// MarshalJSON implement json.Marshaler interface.
func (c Deadman) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			deadmanAlias
			Type string `json:"type"`
		}{
			deadmanAlias: deadmanAlias(c),
			Type:         c.Type(),
		})
}
