package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/flux"
)

// Teams is the notification rule config of Microsoft Teams.
type Teams struct {
	Base
	Title           string `json:"title"`
	MessageTemplate string `json:"messageTemplate"`
	Summary         string `json:"summary"`
}

// GenerateFlux generates a flux script for the teams notification rule.
func (s *Teams) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	teamsEndpoint, ok := e.(*endpoint.Teams)
	if !ok {
		return "", fmt.Errorf("endpoint provided is a %s, not a Teams endpoint", e.Type())
	}
	p, err := s.GenerateFluxAST(teamsEndpoint)
	if err != nil {
		return "", err
	}
	return ast.Format(p), nil
}

// GenerateFluxAST generates a flux AST for the teams notification rule.
func (s *Teams) GenerateFluxAST(e *endpoint.Teams) (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "contrib/sranka/teams", "influxdata/influxdb/secrets", "experimental"),
		s.generateFluxASTBody(e),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *Teams) generateFluxASTBody(e *endpoint.Teams) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTSecrets(e))
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateLevelChecks()...)
	statements = append(statements, s.generateFluxASTNotifyPipe(e))

	return statements
}

func (s *Teams) generateFluxASTSecrets(e *endpoint.Teams) ast.Statement {
	if e.SecretURLSuffix.Key != "" {
		call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.SecretURLSuffix.Key))))
		return flux.DefineVariable("teams_url_suffix", call)
	}
	return flux.DefineVariable("teams_url_suffix", flux.String(""))
}

func (s *Teams) generateFluxASTEndpoint(e *endpoint.Teams) ast.Statement {
	props := []*ast.Property{}
	props = append(props, flux.Property("url", flux.String(e.URL+"${teams_url_suffix}")))
	call := flux.Call(flux.Member("teams", "endpoint"), flux.Object(props...))

	return flux.DefineVariable("teams_endpoint", call)
}

func (s *Teams) generateFluxASTNotifyPipe(e *endpoint.Teams) ast.Statement {
	endpointProps := []*ast.Property{}
	endpointProps = append(endpointProps, flux.Property("title", flux.String(s.Title)))
	endpointProps = append(endpointProps, flux.Property("text", flux.String(s.MessageTemplate)))
	endpointProps = append(endpointProps, flux.Property("summary", flux.String(s.Summary)))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("teams_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

type teamsAlias Teams

// MarshalJSON implement json.Marshaler interface.
func (s Teams) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			teamsAlias
			Type string `json:"type"`
		}{
			teamsAlias: teamsAlias(s),
			Type:       s.Type(),
		})
}

// Valid returns where the config is valid.
func (s Teams) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.Title == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Teams Title is empty",
		}
	}
	if s.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Teams MessageTemplate is empty",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (s Teams) Type() string {
	return "teams"
}
