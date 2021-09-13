package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/flux"
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
		return &errors.Error{
			Code: errors.EInvalid,
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
	return astutil.Format(s.GenerateFluxAST(pagerdutyEndpoint))
}

// GenerateFluxAST generates a flux AST for the pagerduty notification rule.
func (s *PagerDuty) GenerateFluxAST(e *endpoint.PagerDuty) *ast.File {
	return flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "pagerduty", "influxdata/influxdb/secrets", "experimental"),
		s.generateFluxASTBody(e),
	)
}

func (s *PagerDuty) generateFluxASTBody(e *endpoint.PagerDuty) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTSecrets(e))
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateLevelChecks()...)
	statements = append(statements, s.generateFluxASTNotifyPipe(e.ClientURL))

	return statements
}

func (s *PagerDuty) generateFluxASTSecrets(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.RoutingKey.Key))))

	return flux.DefineVariable("pagerduty_secret", call)
}

func (s *PagerDuty) generateFluxASTEndpoint(e *endpoint.PagerDuty) ast.Statement {
	call := flux.Call(flux.Member("pagerduty", "endpoint"),
		flux.Object(),
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
	endpointProps = append(endpointProps, flux.Property("client", flux.String("influxdata")))

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
	endpointProps = append(endpointProps, flux.Property("group", flux.Member("r", "_source_measurement")))

	// severity:
	// required
	// string
	// The perceived severity of the status the event is describing with respect to the affected system. This can be critical, error, warning or info.
	endpointProps = append(endpointProps, flux.Property("severity", severityFromLevel()))

	// event_action:
	// required
	// string trigger
	// The type of event. Can be trigger, acknowledge or resolve. See Event Action.
	endpointProps = append(endpointProps, flux.Property("eventAction", actionFromLevel()))

	// source:
	// required
	// string
	// The unique location of the affected system, preferably a hostname or FQDN
	endpointProps = append(endpointProps, flux.Property("source", flux.Member("notification", "_notification_rule_name")))

	// summary:
	// required
	// string
	// A brief text summary of the event, used to generate the summaries/titles of any associated alerts. The maximum permitted length of this property is 1024 characters.
	endpointProps = append(endpointProps, flux.Property("summary", flux.Member("r", "_message")))

	// timestamp:
	// optional
	// timestamp (rfc3339 milliseconds)
	// The time at which the emitting tool detected or generated the event.
	endpointProps = append(endpointProps, flux.Property("timestamp", generateTime()))

	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("pagerduty_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

func severityFromLevel() *ast.CallExpression {
	return flux.Call(
		flux.Member("pagerduty", "severityFromLevel"),
		flux.Object(
			flux.Property("level", flux.Member("r", "_level")),
		),
	)
}

func actionFromLevel() *ast.CallExpression {
	return flux.Call(
		flux.Member("pagerduty", "actionFromLevel"),
		flux.Object(
			flux.Property("level", flux.Member("r", "_level")),
		),
	)
}

func generateTime() *ast.CallExpression {
	props := []*ast.Property{
		flux.Property("v", flux.Member("r", "_source_timestamp")),
	}
	return flux.Call(flux.Identifier("time"), flux.Object(props...))
}
