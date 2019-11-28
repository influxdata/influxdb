package endpoint

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/notification/flux"

	"github.com/influxdata/influxdb"
)

var _ influxdb.NotificationEndpoint = &Slack{}

const slackTokenSuffix = "-token"

// Slack is the notification endpoint config of slack.
type Slack struct {
	Base
	// URL is a valid slack webhook URL
	// TODO(jm): validate this in unmarshaler
	// example: https://slack.com/api/chat.postMessage
	URL string `json:"url"`
	// Token is the bearer token for authorization
	Token influxdb.SecretField `json:"token"`
	// TestChannel is the channel to test the connection between flux and slack
	TestChannel string `json:"testChannel"`
}

// GenerateTestFlux generates a flux script for the slack notification rule.
func (s *Slack) GenerateTestFlux() (string, error) {
	f, err := s.GenerateTestFluxAST()
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

// GenerateTestFluxAST generates a flux AST for the slack notification rule.
func (s *Slack) GenerateTestFluxAST() (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "slack", "csv"),
		s.generateTestFluxASTBody(),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *Slack) generateTestFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateFluxASTEndpoint())
	statements = append(statements, s.generateFluxASTCSVTable()...)
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *Slack) generateFluxASTEndpoint() ast.Statement {
	props := []*ast.Property{}
	props = append(props, flux.Property("url", flux.String(s.URL)))
	call := flux.Call(flux.Member("slack", "endpoint"), flux.Object(props...))

	return flux.DefineVariable("slack_endpoint", call)
}

func (s *Slack) generateFluxASTCSVTable() []ast.Statement {
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

func (s *Slack) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	messageText := fmt.Sprintf("Your endpoint named %q is working", s.Name)
	endpointProps = append(endpointProps, flux.Property("channel", flux.String(s.TestChannel)))
	endpointProps = append(endpointProps, flux.Property("text", flux.String(messageText)))
	endpointProps = append(endpointProps, flux.Property("color", flux.String("good")))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Object()))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("slack_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("csvTable"), call))
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *Slack) BackfillSecretKeys() {
	if s.Token.Key == "" && s.Token.Value != nil {
		s.Token.Key = s.ID.String() + slackTokenSuffix
	}
}

// SecretFields return available secret fields.
func (s *Slack) SecretFields() []*influxdb.SecretField {
	arr := []*influxdb.SecretField{}
	if s.Token.Key != "" {
		arr = append(arr, &s.Token)
	}
	return arr
}

// Valid returns error if some configuration is invalid
func (s Slack) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.URL == "" && s.Token.Key == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "slack endpoint URL and token are empty",
		}
	}
	if s.URL != "" {
		if _, err := url.Parse(s.URL); err != nil {
			return &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("slack endpoint URL is invalid: %s", err.Error()),
			}
		}
	}
	// TODO(desa): this requirement seems odd
	if s.Token.Key != "" && s.Token.Key != s.ID.String()+slackTokenSuffix {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "slack endpoint token is invalid",
		}
	}
	return nil
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

// Type returns the type.
func (s Slack) Type() string {
	return SlackType
}
