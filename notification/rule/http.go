package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/flux"
)

// HTTP is the notification rule config of http.
type HTTP struct {
	Base
	URL string
}

// GenerateFlux generates a flux script for the http notification rule.
func (s *HTTP) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	// TODO(desa): still needs full implementation
	return s.GenerateFluxReal(e)
}

// GenerateFluxReal generates a flux script for the http notification rule.
func (s *HTTP) GenerateFluxReal(e influxdb.NotificationEndpoint) (string, error) {
	p, err := s.GenerateFluxAST()
	if err != nil {
		return "", err
	}
	return ast.Format(p), nil
}

// GenerateFluxAST generates a flux AST for the http notification rule.
func (s *HTTP) GenerateFluxAST() (*ast.Package, error) {
	f := flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/alerts", "http"),
		s.generateFluxASTBody(),
	)
	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (s *HTTP) generateFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	statements = append(statements, s.generateFluxASTEndpoint())
	statements = append(statements, s.generateFluxASTNotificationDefinition())
	statements = append(statements, s.generateFluxASTStatusPipe())
	statements = append(statements, s.generateFluxASTNotifyPipe())

	return statements
}

func (s *HTTP) generateTaskOption() ast.Statement {
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

func (s *HTTP) generateFluxASTNotificationDefinition() ast.Statement {
	// TODO(desa): what else needs to be here?
	id := flux.Property("notificationID", flux.String(s.ID.String()))
	name := flux.Property("name", flux.String(s.Name))

	return flux.DefineVariable("notification", flux.Object(id, name))
}

func (s *HTTP) generateFluxASTEndpoint() ast.Statement {
	// TODO(desa): where does <some key> come from
	call := flux.Call(flux.Member("http", "endpoint"), flux.Object(flux.Property("url", flux.String(s.URL))))

	return flux.DefineVariable("http_endpoint", call)
}

func (s *HTTP) generateFluxASTStatusPipe() ast.Statement {
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

func (s *HTTP) generateFluxASTNotifyPipe() ast.Statement {
	endpointProps := []*ast.Property{}
	endpointBody := flux.Call(flux.Member("json", "encode"), flux.Object(flux.Property("v", flux.Identifier("r"))))
	endpointProps = append(endpointProps, flux.Property("data", endpointBody))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("name", flux.String(s.Name)))
	props = append(props, flux.Property("notification", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("http_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("alerts", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("statuses"), call))
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
