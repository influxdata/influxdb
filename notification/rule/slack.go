package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/flux"
)

// Slack is the notification rule config of slack.
type Slack struct {
	Base
	Channel         string `json:"channel"`
	MessageTemplate string `json:"messageTemplate"`
}

// GenerateFlux generates a flux script for the slack notification rule.
func (s *Slack) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	slackEndpoint, ok := e.(*endpoint.Slack)
	if !ok {
		return "", fmt.Errorf("endpoint provided is a %s, not an Slack endpoint", e.Type())
	}
	return astutil.Format(s.GenerateFluxAST(slackEndpoint))
}

// GenerateFluxAST generates a flux AST for the slack notification rule.
func (s *Slack) GenerateFluxAST(e *endpoint.Slack) *ast.File {
	return flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "slack", "influxdata/influxdb/secrets", "experimental"),
		s.generateFluxASTBody(e),
	)
}

func (s *Slack) generateFluxASTBody(e *endpoint.Slack) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	if e.Token.Key != "" {
		statements = append(statements, s.generateFluxASTSecrets(e))
	}
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateLevelChecks()...)
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *Slack) generateFluxASTSecrets(e *endpoint.Slack) ast.Statement {
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.Token.Key))))

	return flux.DefineVariable("slack_secret", call)
}

func (s *Slack) generateFluxASTEndpoint(e *endpoint.Slack) ast.Statement {
	props := []*ast.Property{}
	if e.Token.Key != "" {
		props = append(props, flux.Property("token", flux.Identifier("slack_secret")))
	}
	if e.URL != "" {
		props = append(props, flux.Property("url", flux.String(e.URL)))
	}
	call := flux.Call(flux.Member("slack", "endpoint"), flux.Object(props...))

	return flux.DefineVariable("slack_endpoint", call)
}

func (s *Slack) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	endpointProps = append(endpointProps, flux.Property("channel", flux.String(s.Channel)))
	// TODO(desa): are these values correct?
	endpointProps = append(endpointProps, flux.Property("text", flux.String(s.MessageTemplate)))
	endpointProps = append(endpointProps, flux.Property("color", s.generateSlackColors()))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("slack_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

func (s *Slack) generateSlackColors() ast.Expression {
	level := flux.Member("r", "_level")
	return flux.If(
		flux.Equal(level, flux.String("crit")),
		flux.String("danger"),
		flux.If(
			flux.Equal(level, flux.String("warn")),
			flux.String("warning"),
			flux.String("good"),
		),
	)
}

type slackAlias Slack

// MarshalJSON implement json.Marshaler interface.
func (s Slack) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			slackAlias
			Type string `json:"type"`
		}{
			slackAlias: slackAlias(s),
			Type:       s.Type(),
		})
}

// Valid returns where the config is valid.
func (s Slack) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "slack msg template is empty",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (s Slack) Type() string {
	return "slack"
}
