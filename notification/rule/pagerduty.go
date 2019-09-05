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
func (s PagerDuty) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			pagerDutyAlias
			Type string `json:"type"`
		}{
			pagerDutyAlias: pagerDutyAlias(s),
			Type:           s.Type(),
		})
}

// Valid returns where the config is valid.
func (s PagerDuty) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.MessageTemplate == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "pagerduty invalid message template",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (s PagerDuty) Type() string {
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
	statements = append(statements, s.generateFluxASTNotifyPipe(e.URL))

	return statements
}

func (s *PagerDuty) generateFluxASTSecrets(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.RoutingKey.Key))))

	return flux.DefineVariable("pagerduty_secret", call)
}

func (s *PagerDuty) generateFluxASTEndpoint(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("pagerduty", "endpoint"),
		flux.Object(
			flux.Property("token", flux.Identifier("pagerduty_secret")),
			flux.Property("url", flux.String(e.URL)),
		),
	)

	return flux.DefineVariable("pagerduty_endpoint", call)
}

func (s *PagerDuty) generateFluxASTNotifyPipe(url string) ast.Statement {
	endpointProps := []*ast.Property{}

	// routing_key:
	// required
	// string
	// A version 4 UUID expressed as a 32-digit hexadecimal number. This is the Integration Key for an integration on any given service.
	endpointProps = append(endpointProps, flux.Property("routingKey", flux.Identifier("pagerduty_secret")))

	// client:
	// optional
	// string
	// name of the client sending the alert.
	endpointProps = append(endpointProps, flux.Property("client", flux.Identifier("r._check_name")))

	// clientURL
	// optional
	// string
	// url of the client sending the alert.
	endpointProps = append(endpointProps, flux.Property("clientURL", flux.String(url)))

	// class:
	// optional
	// string
	// The class/type of the event, for example ping failure or cpu load
	endpointProps = append(endpointProps, flux.Property("class", flux.Identifier("r._check_name")))

	// group:
	// optional
	// string
	// Logical grouping of components of a service, for example app-stack
	endpointProps = append(endpointProps, flux.Property("group", flux.Identifier("r._check_name")))

	// severity:
	// required
	// string
	// The perceived severity of the status the event is describing with respect to the affected system. This can be critical, error, warning or info.
	// TODO: transfrom the influx names to pagerduty names
	endpointProps = append(endpointProps, flux.Property("severity", flux.Identifier("r._level")))

	// source:
	// required
	// string
	// The unique location of the affected system, preferably a hostname or FQDN
	endpointProps = append(endpointProps, flux.Property("source", flux.Identifier("r._source_measurement")))

	// summary:
	// required
	// string
	// A brief text summary of the event, used to generate the summaries/titles of any associated alerts. The maximum permitted length of this property is 1024 characters.
	endpointProps = append(endpointProps, flux.Property("summary", flux.Identifier("r._message")))

	// timestamp:
	// optional
	// timestamp (rfc3339 milliseconds)
	// The time at which the emitting tool detected or generated the event.
	// TODO: this should be r._status_timestamp
	endpointProps = append(endpointProps, flux.Property("timestamp", flux.Identifier("r._status_timestamp")))

	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("pagerduty_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("statuses"), call))
}
