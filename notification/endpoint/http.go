package endpoint

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/flux"
)

var _ influxdb.NotificationEndpoint = &HTTP{}

const (
	httpTokenSuffix    = "-token"
	httpUsernameSuffix = "-username"
	httpPasswordSuffix = "-password"
)

// HTTP is the notification endpoint config of http.
type HTTP struct {
	Base
	// Path is the API path of HTTP
	URL string `json:"url"`
	// Token is the bearer token for authorization
	Headers         map[string]string    `json:"headers,omitempty"`
	Token           influxdb.SecretField `json:"token,omitempty"`
	Username        influxdb.SecretField `json:"username,omitempty"`
	Password        influxdb.SecretField `json:"password,omitempty"`
	AuthMethod      string               `json:"authMethod"`
	Method          string               `json:"method"`
	ContentTemplate string               `json:"contentTemplate"`
}

// GenerateTestFlux generates the flux that tests PagerDuty endpoints
func (h *HTTP) GenerateTestFlux() (string, error) {
	f, err := h.GenerateTestFluxAST()
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
func (h *HTTP) GenerateTestFluxAST() (*ast.Package, error) {
	f := flux.File(
		h.Name,
		h.imports(),
		h.generateTestFluxASTBody(),
	)

	return &ast.Package{Package: "main", Files: []*ast.File{f}}, nil
}

func (h *HTTP) imports() []*ast.ImportDeclaration {
	packages := []string{
		"influxdata/influxdb/monitor",
		"http",
		"csv",
		"json",
	}

	if h.AuthMethod == "bearer" || h.AuthMethod == "basic" {
		packages = append(packages, "influxdata/influxdb/secrets")
	}

	return flux.Imports(packages...)
}

func (h *HTTP) generateTestFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, h.generateFluxASTHeaders())
	statements = append(statements, h.generateFluxASTEndpoint())
	statements = append(statements, h.generateFluxASTCSVTable()...)
	statements = append(statements, h.generateFluxASTNotifyPipe())

	return statements
}

func (h *HTTP) generateFluxASTHeaders() ast.Statement {
	props := []*ast.Property{
		flux.Dictionary("Content-Type", flux.String("application/json")),
	}

	switch h.AuthMethod {
	case "bearer":
		token := flux.Call(
			flux.Member("secrets", "get"),
			flux.Object(
				flux.Property("key", flux.String(h.Token.Key)),
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
				flux.Property("key", flux.String(h.Username.Key)),
			),
		)
		passwd := flux.Call(
			flux.Member("secrets", "get"),
			flux.Object(
				flux.Property("key", flux.String(h.Password.Key)),
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

func (h *HTTP) generateFluxASTEndpoint() ast.Statement {
	props := []*ast.Property{}
	props = append(props, flux.Property("url", flux.String(h.URL)))
	call := flux.Call(flux.Member("http", "endpoint"), flux.Object(props...))

	return flux.DefineVariable("http_endpoint", call)
}

func (h *HTTP) generateFluxASTCSVTable() []ast.Statement {
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

func (s *HTTP) generateFluxASTNotifyPipe() ast.Statement {
	endpointBody := flux.Call(
		flux.Member("json", "encode"),
		flux.Object(flux.Property("v", flux.Identifier("body"))),
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
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("http_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("csvTable"), call))
}

func (s *HTTP) generateBody() ast.Statement {
	// {r with "_version": 1}
	props := []*ast.Property{
		flux.Property(
			"_version", flux.Integer(1),
		),
	}

	body := flux.ObjectWith("r", props...)
	return flux.DefineVariable("body", body)
}

// BackfillSecretKeys fill back fill the secret field key during the unmarshalling
// if value of that secret field is not nil.
func (s *HTTP) BackfillSecretKeys() {
	if s.Token.Key == "" && s.Token.Value != nil {
		s.Token.Key = s.ID.String() + httpTokenSuffix
	}
	if s.Username.Key == "" && s.Username.Value != nil {
		s.Username.Key = s.ID.String() + httpUsernameSuffix
	}
	if s.Password.Key == "" && s.Password.Value != nil {
		s.Password.Key = s.ID.String() + httpPasswordSuffix
	}
}

// SecretFields return available secret fields.
func (s *HTTP) SecretFields() []*influxdb.SecretField {
	arr := make([]*influxdb.SecretField, 0)
	if s.Token.Key != "" {
		arr = append(arr, &s.Token)
	}
	if s.Username.Key != "" {
		arr = append(arr, &s.Username)
	}
	if s.Password.Key != "" {
		arr = append(arr, &s.Password)
	}
	return arr
}

var goodHTTPAuthMethod = map[string]bool{
	"none":   true,
	"basic":  true,
	"bearer": true,
}

var goodHTTPMethod = map[string]bool{
	http.MethodGet:  true,
	http.MethodPost: true,
	http.MethodPut:  true,
}

// Valid returns error if some configuration is invalid
func (s HTTP) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.URL == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "http endpoint URL is empty",
		}
	}
	if _, err := url.Parse(s.URL); err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("http endpoint URL is invalid: %s", err.Error()),
		}
	}
	if !goodHTTPMethod[s.Method] {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid http http method",
		}
	}
	if !goodHTTPAuthMethod[s.AuthMethod] {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid http auth method",
		}
	}
	if s.AuthMethod == "basic" &&
		(s.Username.Key != s.ID.String()+httpUsernameSuffix ||
			s.Password.Key != s.ID.String()+httpPasswordSuffix) {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid http username/password for basic auth",
		}
	}
	if s.AuthMethod == "bearer" && s.Token.Key != s.ID.String()+httpTokenSuffix {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid http token for bearer auth",
		}
	}

	return nil
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

// Type returns the type.
func (s HTTP) Type() string {
	return HTTPType
}

// ParseResponse will parse the http response from http.
func (s HTTP) ParseResponse(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return &influxdb.Error{
			Msg: string(body),
		}
	}
	return nil
}
