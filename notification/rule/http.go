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
	statements = append(statements, s.generateHeaders(e))
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateAllStateChanges()...)
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *HTTP) generateHeaders(e *endpoint.HTTP) ast.Statement {
	props := []*ast.Property{
		flux.Dictionary(
			"Content-Type", flux.String("application/json"),
		),
	}

	switch e.AuthMethod {
	case "bearer":
		token := flux.Call(
			flux.Member("secrets", "get"),
			flux.Object(
				flux.Property("key", flux.String(e.Token.Key)),
			),
		)
		bearer := flux.Add(
			flux.String("Bearer "),
			token,
		)
		auth := flux.Dictionary("Authorization", bearer)
		props = append(props, auth)
	case "basic":
		username := flux.Call(
			flux.Member("secrets", "get"),
			flux.Object(
				flux.Property("key", flux.String(e.Username.Key)),
			),
		)
		passwd := flux.Call(
			flux.Member("secrets", "get"),
			flux.Object(
				flux.Property("key", flux.String(e.Password.Key)),
			),
		)

		basic := flux.Call(
			flux.Member("http", "basicAuth"),
			flux.Object(
				flux.Property("u", username),
				flux.Property("p", passwd),
			),
		)

		auth := flux.Dictionary("Authorization", basic)
		props = append(props, auth)
	}
	return flux.DefineVariable("headers", flux.Object(props...))
}

func (s *HTTP) generateFluxASTEndpoint(e *endpoint.HTTP) ast.Statement {
	// TODO(desa): where does <some key> come from
	call := flux.Call(flux.Member("http", "endpoint"), flux.Object(flux.Property("url", flux.String(e.URL))))

	return flux.DefineVariable("endpoint", call)
}

func (s *HTTP) generateFluxASTNotifyPipe() ast.Statement {
	endpointBody := flux.Call(
		flux.Member("json", "encode"),
		flux.Object(flux.Property("v", flux.Identifier("r"))),
	)
	headers := flux.Property("headers", flux.Identifier("headers"))

	endpointProps := []*ast.Property{
		headers,
		flux.Property("data", endpointBody),
	}
	endpointFn := flux.FuncBlock(flux.FunctionParams("r"),
		s.generateBody(),
		&ast.ReturnStatement{
			Argument: flux.Object(endpointProps...),
		},
	)

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

func (s *HTTP) generateBody() ast.Statement {
	props := []*ast.Property{
		flux.Dictionary(
			"version", flux.Integer(1),
		),
		flux.Dictionary(
			"rule_name", flux.Member("notification", "_notification_rule_name"),
		),
		flux.Dictionary(
			"rule_id", flux.Member("notification", "_notification_rule_id"),
		),
		flux.Dictionary(
			"endpoint_name", flux.Member("notification", "_notification_endpoint_name"),
		),
		flux.Dictionary(
			"endpoint_id", flux.Member("notification", "_notification_endpoint_id"),
		),
		flux.Dictionary(
			"check_name", flux.Member("r", "_check_name"),
		),
		flux.Dictionary(
			"check_id", flux.Member("r", "_check_id"),
		),
		flux.Dictionary(
			"check_type", flux.Member("r", "_type"),
		),
		flux.Dictionary(
			"source_measurement", flux.Member("r", "_source_measurement"),
		),
		flux.Dictionary(
			"source_timestamp", flux.Member("r", "_source_timestamp"),
		),
		flux.Dictionary(
			"level", flux.Member("r", "_level"),
		),
		flux.Dictionary(
			"message", flux.Member("r", "_message"),
		),
	}
	return flux.DefineVariable("body", flux.Object(props...))
}

type httpAlias HTTP

// MarshalJSON implement json.Marshaler interface.
func (s HTTP) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			httpAlias
			Type string `json:"type"`
		}{
			httpAlias: httpAlias(s),
			Type:      s.Type(),
		})
}

// Valid returns where the config is valid.
func (s HTTP) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	return nil
}

// Type returns the type of the rule config.
func (s HTTP) Type() string {
	return "http"
}
