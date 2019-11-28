package endpoint

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/flux"
)

var _ influxdb.NotificationEndpoint = &PagerDuty{}

const routingKeySuffix = "-routing-key"

// PagerDuty is the notification endpoint config of pagerduty.
type PagerDuty struct {
	Base
	// ClientURL is the url that is presented in the PagerDuty UI when this alert is triggered
	ClientURL string `json:"clientURL"`
	// RoutingKey is a version 4 UUID expressed as a 32-digit hexadecimal number.
	// This is the Integration Key for an integration on any given service.
	RoutingKey influxdb.SecretField `json:"routingKey"`
}

// GenerateTestFlux generates a flux script for the pagerduty notification rule.
func (p *PagerDuty) GenerateTestFlux() (string, error) {
	f, err := p.GenerateFluxAST()
	if err != nil {
		return "", err
	}

	// used string interpolation here because flux's ast.Format returns malformed
	// multi-line strings 😿
	data := `#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0`

	return fmt.Sprintf(ast.Format(f), data), nil
}

// GenerateFluxAST generates a flux AST for the pagerduty notification rule.
func (p *PagerDuty) GenerateFluxAST() (*ast.Package, error) {
	f := flux.File(
		p.Name,
		flux.Imports("influxdata/influxdb/monitor", "pagerduty", "csv", "influxdata/influxdb/secrets"),
		p.generateFluxASTBody(),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (p *PagerDuty) generateFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, p.generateFluxASTEndpoint())
	statements = append(statements, p.generateFluxASTCSVTable()...)
	statements = append(statements, p.generateFluxASTNotifyPipe())

	return statements
}

func (p *PagerDuty) generateFluxASTEndpoint() ast.Statement {
	call := flux.Call(flux.Member("pagerduty", "endpoint"),
		flux.Object(),
	)

	return flux.DefineVariable("pagerduty_endpoint", call)
}

func (p *PagerDuty) generateFluxASTCSVTable() []ast.Statement {
	props := []*ast.Property{}
	// used string interpolation here because flux's ast.Format returns
	// malformed multi-line strings 😿
	data := flux.DefineVariable("data", flux.String("%s"))
	props = append(props, flux.Property("csv", flux.Identifier("data")))
	call := flux.Call(flux.Member("csv", "from"), flux.Object(props...))
	table := flux.DefineVariable("csvTable", call)

	return []ast.Statement{
		data,
		table,
	}
}

func (p *PagerDuty) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}

	// routing_key:
	// required
	// string
	// A version 4 UUID expressed as a 32-digit hexadecimal number. This is the Integration Key for an integration on any given service.
	endpointProps = append(endpointProps, flux.Property("routingKey", flux.String(*p.RoutingKey.Value)))

	// clientURL
	// optional
	// string
	// url of the client sending the alert.
	endpointProps = append(endpointProps, flux.Property("clientURL", flux.String(p.ClientURL)))

	// severity:
	// required
	// string
	// The perceived severity of the status the event is describing with respect to the affected system. This can be critical, error, warning or info.
	endpointProps = append(endpointProps, flux.Property("severity", flux.String("info")))

	// event_action:
	// required
	// string trigger
	// The type of event. Can be trigger, acknowledge or resolve. See Event Action.
	endpointProps = append(endpointProps, flux.Property("eventAction", flux.String("trigger")))

	// source:
	// required
	// string
	// The unique location of the affected system, preferably a hostname or FQDN
	endpointProps = append(endpointProps, flux.Property("source", flux.String("pager_duty_test")))

	// summary:
	// required
	// string
	// A brief text summary of the event, used to generate the summaries/titles of any associated alerts. The maximum permitted length of this property is 1024 characters.
	endpointProps = append(endpointProps, flux.Property("summary", flux.String("PagerDuty connection successful")))

	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("pagerduty_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("csvTable"), call))
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *PagerDuty) BackfillSecretKeys() {
	if s.RoutingKey.Key == "" && s.RoutingKey.Value != nil {
		s.RoutingKey.Key = s.ID.String() + routingKeySuffix
	}
}

// SecretFields return available secret fields.
func (s *PagerDuty) SecretFields() []*influxdb.SecretField {
	return []*influxdb.SecretField{
		&s.RoutingKey,
	}
}

// Valid returns error if some configuration is invalid
func (s PagerDuty) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.RoutingKey.Key != s.ID.String()+routingKeySuffix {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "pagerduty routing key is invalid",
		}
	}
	return nil
}

type pagerdutyAlias PagerDuty

// MarshalJSON implement json.Marshaler interface.
func (s PagerDuty) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			pagerdutyAlias
			Type string `json:"type"`
		}{
			pagerdutyAlias: pagerdutyAlias(s),
			Type:           s.Type(),
		})
}

// Type returns the type.
func (s PagerDuty) Type() string {
	return PagerDutyType
}
