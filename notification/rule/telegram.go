package rule

import (
	"encoding/json"
	"fmt"

	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/flux"
)

// Telegram is the notification rule config of telegram.
type Telegram struct {
	Base
	MessageTemplate       string `json:"messageTemplate"`
	ParseMode             string `json:"parseMode"`
	DisableWebPagePreview bool   `json:"disableWebPagePreview"`
}

// GenerateFlux generates a flux script for the telegram notification rule.
func (s *Telegram) GenerateFlux(e influxdb.NotificationEndpoint) (string, error) {
	telegramEndpoint, ok := e.(*endpoint.Telegram)
	if !ok {
		return "", fmt.Errorf("endpoint provided is a %s, not a Telegram endpoint", e.Type())
	}
	return astutil.Format(s.GenerateFluxAST(telegramEndpoint))
}

// GenerateFluxAST generates a flux AST for the telegram notification rule.
func (s *Telegram) GenerateFluxAST(e *endpoint.Telegram) *ast.File {
	return flux.File(
		s.Name,
		flux.Imports("influxdata/influxdb/monitor", "contrib/sranka/telegram", "influxdata/influxdb/secrets", "experimental"),
		s.generateFluxASTBody(e),
	)
}

func (s *Telegram) generateFluxASTBody(e *endpoint.Telegram) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, s.generateTaskOption())
	if e.Token.Key != "" {
		statements = append(statements, s.generateFluxASTSecrets(e))
	}
	statements = append(statements, s.generateFluxASTEndpoint(e))
	statements = append(statements, s.generateFluxASTNotificationDefinition(e))
	statements = append(statements, s.generateFluxASTStatuses())
	statements = append(statements, s.generateLevelChecks()...)
	statements = append(statements, s.generateFluxASTNotifyPipe(e))

	return statements
}

func (s *Telegram) generateFluxASTSecrets(e *endpoint.Telegram) ast.Statement {
	call := flux.Call(flux.Member("secrets", "get"), flux.Object(flux.Property("key", flux.String(e.Token.Key))))

	return flux.DefineVariable("telegram_secret", call)
}

func (s *Telegram) generateFluxASTEndpoint(e *endpoint.Telegram) ast.Statement {
	props := []*ast.Property{}
	if e.Token.Key != "" {
		props = append(props, flux.Property("token", flux.Identifier("telegram_secret")))
	}
	if s.ParseMode != "" {
		props = append(props, flux.Property("parseMode", flux.String(s.ParseMode)))
	}
	props = append(props, flux.Property("disableWebPagePreview", flux.Bool(s.DisableWebPagePreview)))
	call := flux.Call(flux.Member("telegram", "endpoint"), flux.Object(props...))

	return flux.DefineVariable("telegram_endpoint", call)
}

func (s *Telegram) generateFluxASTNotifyPipe(e *endpoint.Telegram) ast.Statement {
	endpointProps := []*ast.Property{}
	endpointProps = append(endpointProps, flux.Property("channel", flux.String(e.Channel)))
	endpointProps = append(endpointProps, flux.Property("text", flux.String(s.MessageTemplate)))
	endpointProps = append(endpointProps, flux.Property("silent", s.generateSilent()))
	endpointFn := flux.Function(flux.FunctionParams("r"), flux.Object(endpointProps...))

	props := []*ast.Property{}
	props = append(props, flux.Property("data", flux.Identifier("notification")))
	props = append(props, flux.Property("endpoint",
		flux.Call(flux.Identifier("telegram_endpoint"), flux.Object(flux.Property("mapFn", endpointFn)))))

	call := flux.Call(flux.Member("monitor", "notify"), flux.Object(props...))

	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("all_statuses"), call))
}

func (s *Telegram) generateSilent() ast.Expression {
	level := flux.Member("r", "_level")
	return flux.If(
		flux.Equal(level, flux.String("crit")),
		flux.Bool(true),
		flux.If(
			flux.Equal(level, flux.String("warn")),
			flux.Bool(true),
			flux.Bool(false),
		),
	)
}

type telegramAlias Telegram

// MarshalJSON implement json.Marshaler interface.
func (s Telegram) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			telegramAlias
			Type string `json:"type"`
		}{
			telegramAlias: telegramAlias(s),
			Type:          s.Type(),
		})
}

// Valid returns where the config is valid.
func (s Telegram) Valid() error {
	if err := s.Base.valid(); err != nil {
		return err
	}
	if s.MessageTemplate == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Telegram MessageTemplate is invalid",
		}
	}
	return nil
}

// Type returns the type of the rule config.
func (s Telegram) Type() string {
	return "telegram"
}
