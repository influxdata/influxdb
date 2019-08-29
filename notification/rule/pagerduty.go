package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/flux"
)

// PagerDuty is the rule config of pagerduty notification.
type PagerDuty struct {
	Base
	MessageTemplate string `json:"messageTemplate"`
}

type pagerDutyAlias PagerDuty

// MarshalJSON implement json.Marshaler interface.
func (c PagerDuty) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			pagerDutyAlias
			Type string `json:"type"`
		}{
			pagerDutyAlias: pagerDutyAlias(c),
			Type:           c.Type(),
		})
}

// Valid returns where the config is valid.
func (c PagerDuty) Valid() error {
	if err := c.Base.valid(); err != nil {
		return err
	}
	if c.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "pagerduty invalid message template",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (c PagerDuty) Type() string {
	return "pagerduty"
}

// GenerateFlux generates a flux script for the pagerduty notification rule.
func (s *PagerDuty) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	pagerdutyEndpoint, ok := e.(*endpoint.PagerDuty)
	if !ok {
		return "", fmt.Errorf("endpoint provided is a %s, not an PagerDuty endpoint", e.Type())
	}
	p, err := s.GenerateFluxAST(pagerdutyEndpoint)
	if err != nil {
		return "", err
	}
	return ast.Format(p), nil
}

// GenerateFluxAST generates a flux AST for the pagerduty notification rule.
func (s *PagerDuty) GenerateFluxAST(e *endpoint.PagerDuty) (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "pagerduty", "influxdata/influxdb/secrets"),
		s.generateFluxASTBody(e),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *PagerDuty) generateFluxASTBody(e *endpoint.PagerDuty) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTSecrets(e))
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *PagerDuty) generateFluxASTSecrets(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.RoutingKey.Key))))

	return flux.DefineVariable("pagerduty_secret", call)
}

func (s *PagerDuty) generateFluxASTEndpoint(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("pagerduty", "endpoint"),
		flux.Object(
			flux.Property("routing_key", flux.Identifier("pagerduty_secret")),
			flux.Property("url", flux.String(e.URL)),
		),
	)

	return flux.DefineVariable("pagerduty_endpoint", call)
}

func (s *PagerDuty) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	// TODO(desa): are these values correct?
	endpointProps = append(endpointProps, flux.Property("text", flux.String(s.MessageTemplate)))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("pagerduty_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("statuses"), call))
}
