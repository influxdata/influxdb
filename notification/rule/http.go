package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/flux"
)

// HTTP is the notification rule config of http.
type HTTP struct {
	Base
}

// GenerateFlux generates a flux script for the http notification rule.
func (s *HTTP) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	httpEndpoint, ok := e.(*endpoint.HTTP)
	if !ok {
		return "", fmt.Errorf("endpoint provided is a %s, not an HTTP endpoint", e.Type())
	}
	p, err := s.GenerateFluxAST(httpEndpoint)
	if err != nil {
		return "", err
	}
	return ast.Format(p), nil
}

// GenerateFluxAST generates a flux AST for the http notification rule.
func (s *HTTP) GenerateFluxAST(e *endpoint.HTTP) (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "http", "json", "experimental"),
		s.generateFluxASTBody(e),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *HTTP) generateFluxASTBody(e *endpoint.HTTP) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateAllStateChanges()...)
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *HTTP) generateFluxASTEndpoint(e *endpoint.HTTP) ast.Statement {
	// TODO(desa): where does <some key> come from
	call := flux.Call(flux.Member("http", "endpoint"), flux.Object(flux.Property("url", flux.String(e.URL))))

	return flux.DefineVariable("endpoint", call)
}

func (s *HTTP) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	endpointBody := flux.Call(flux.Member("json", "encode"), flux.Object(flux.Property("v", flux.Identifier("r"))))
	endpointProps = append(endpointProps, flux.Property("data", endpointBody))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

type httpAlias HTTP

// MarshalJSON implement json.Marshaler interface.
func (c HTTP) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			httpAlias
			Type string `json:"type"`
		}{
			httpAlias: httpAlias(c),
			Type:      c.Type(),
		})
}

// Valid returns where the config is valid.
func (c HTTP) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	return nil
}

// Type returns the type of the rule config.
func (c HTTP) Type() string {
	return "http"
}
