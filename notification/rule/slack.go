package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/flux"
)

// Slack is the notification rule config of slack.
type Slack struct {
	Base
	Channel         string `json:"channel"`
	MessageTemplate string `json:"messageTemplate"`
}

// GenerateFlux generates a flux script for the slack notification rule.
func (s *Slack) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	// TODO(desa): needs implementation
	return `package main
data = from(bucket: "telegraf")
	|> range(start: -1m)

option task = {name: "name1", every: 1m}`, nil
}

// GenerateFluxReal generates a flux script for the slack notification rule.
func (s *Slack) GenerateFluxReal(e influxdb.NotificationEndpoint) (string, error) {
	p, err := s.GenerateFluxAST()
	if err != nil {
		return "", err
	}
	return ast.Format(p), nil
}

// GenerateFluxAST generates a flux AST for the slack notification rule.
func (s *Slack) GenerateFluxAST() (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "slack", "secrets"),
		s.generateFluxASTBody(),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *Slack) generateFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTSecrets())
	statements = append(statements, s.generateFluxASTEndpoint())
	statements = append(statements, s.generateFluxASTNotificationDefinition())
	statements = append(statements, s.generateFluxASTStatusPipe())
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *Slack) generateTaskOption() ast.Statement {
	props := []*ast.Property{}

	props = append(props, flux.Property("name", flux.String(s.Name)))

	if s.Cron != "" {
		props = append(props, flux.Property("cron", flux.String(s.Cron)))
	}

	if s.Every != nil {
		props = append(props, flux.Property("every", (*ast.DurationLiteral)(s.Every)))
	}

	if s.Offset != nil {
		props = append(props, flux.Property("offset", (*ast.DurationLiteral)(s.Offset)))
	}

	return flux.DefineTaskOption(flux.Object(props...))
}

func (s *Slack) generateFluxASTNotificationDefinition() ast.Statement {
	// TODO(desa): what else needs to be here?
	id := flux.Property("notificationID", flux.String(s.ID.String()))
	name := flux.Property("name", flux.String(s.Name))

	return flux.DefineVariable("notification", flux.Object(id, name))
}

func (s *Slack) generateFluxASTSecrets() ast.Statement {
	// TODO(desa): where does <some key> come from
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String("<secrets key>"))))

	return flux.DefineVariable("slack_secret", call)
}

func (s *Slack) generateFluxASTEndpoint() ast.Statement {
	// TODO(desa): where does <some key> come from
	call := flux.Call(flux.Member("slack", "endpoint"), flux.Object(flux.Property("token", flux.Identifier("slack_secret"))))

	return flux.DefineVariable("slack_endpoint", call)
}

func (s *Slack) generateFluxASTStatusPipe() ast.Statement {
	base := flux.Call(flux.Identifier("from"), flux.Object(flux.Property("bucket", flux.String("system_bucket"))))

	calls := []*ast.CallExpression{}

	// TODO(desa): make start negative of every
	calls = append(calls, flux.Call(flux.Identifier("range"), flux.Object(flux.Property("start", (*ast.DurationLiteral)(s.Every)))))

	for _, r := range s.TagRules {
		switch r.Operator {
		case notification.Equal:
			fn := flux.Function(flux.FunctionParams("r"), flux.Equal(flux.Member("r", r.Key), flux.String(r.Value)))
			calls = append(calls, flux.Call(flux.Identifier("filter"), flux.Object(flux.Property("fn", fn))))
		default:
			// TODO(desa): have this work for all operator types
			panic(fmt.Sprintf("operator %v not currently supported", r.Operator))
		}
	}

	return flux.DefineVariable("statuses", flux.Pipe(base, calls...))
}

func (s *Slack) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	endpointProps = append(endpointProps, flux.Property("channel", flux.String(s.Channel)))
	// TODO(desa): are these values correct?
	endpointProps = append(endpointProps, flux.Property("text", flux.String(s.MessageTemplate)))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("name", flux.String(s.Name)))
	props = append(props, flux.Property("notification", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("slack_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("statuses"), call))
}

type slackAlias Slack

// MarshalJSON implement json.Marshaler interface.
func (c Slack) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			slackAlias
			Type string `json:"type"`
		}{
			slackAlias: slackAlias(c),
			Type:       c.Type(),
		})
}

// Valid returns where the config is valid.
func (c Slack) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	if c.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "slack msg template is empty",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (c Slack) Type() string {
	return "slack"
}
