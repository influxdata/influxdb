package check

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/flux"
)

var _ influxdb.Check = &Threshold{}

// Threshold is the threshold check.
type Threshold struct {
	Base
	Thresholds []ThresholdConfig `json:"thresholds"`
}

// Type returns the type of the check.
func (t Threshold) Type() string {
	return "threshold"
}

// Valid returns error if something is invalid.
func (t Threshold) Valid() error {
	if err := t.Base.Valid(); err != nil {
		return err
	}
	for _, cc := range t.Thresholds {
		if err := cc.Valid(); err != nil {
			return err
		}
	}
	return nil
}

type thresholdDecode struct {
	Base
	Thresholds []thresholdConfigDecode `json:"thresholds"`
}

type thresholdConfigDecode struct {
	ThresholdConfigBase
	Type   string  `json:"type"`
	Value  float64 `json:"value"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Within bool    `json:"within"`
}

// UnmarshalJSON implement json.Unmarshaler interface.
func (t *Threshold) UnmarshalJSON(b []byte) error {
	tdRaws := new(thresholdDecode)
	if err := json.Unmarshal(b, tdRaws); err != nil {
		return err
	}
	t.Base = tdRaws.Base
	for _, tdRaw := range tdRaws.Thresholds {
		switch tdRaw.Type {
		case "lesser":
			td := &Lesser{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Value:               tdRaw.Value,
			}
			t.Thresholds = append(t.Thresholds, td)
		case "greater":
			td := &Greater{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Value:               tdRaw.Value,
			}
			t.Thresholds = append(t.Thresholds, td)
		case "range":
			td := &Range{
				ThresholdConfigBase: tdRaw.ThresholdConfigBase,
				Min:                 tdRaw.Min,
				Max:                 tdRaw.Max,
				Within:              tdRaw.Within,
			}
			t.Thresholds = append(t.Thresholds, td)
		default:
			return &influxdb.Error{
				Msg: fmt.Sprintf("invalid threshold type %s", tdRaw.Type),
			}
		}
	}

	return nil
}

func multiError(errs []error) error {
	var b strings.Builder

	for _, err := range errs {
		b.WriteString(err.Error() + "\n")
	}

	return fmt.Errorf(b.String())
}

// GenerateFlux returns a flux script for the threshold provided. If there
// are any errors in the flux that the user provided the function will return
// an error for each error found when the script is parsed.
func (t Threshold) GenerateFlux() (string, error) {
	p, err := t.GenerateFluxAST()
	if err != nil {
		return "", err
	}

	return ast.Format(p), nil
}

// GenerateFluxAST returns a flux AST for the threshold provided. If there
// are any errors in the flux that the user provided the function will return
// an error for each error found when the script is parsed.
func (t Threshold) GenerateFluxAST() (*ast.Package, error) {
	p := parser.ParseSource(t.Query.Text)

	if errs := ast.GetErrors(p); len(errs) != 0 {
		return nil, multiError(errs)
	}

	// TODO(desa): this is a hack that we had to do as a result of https://github.com/influxdata/flux/issues/1701
	// when it is fixed we should use a separate file and not manipulate the existing one.
	if len(p.Files) != 1 {
		return nil, fmt.Errorf("expect a single file to be returned from query parsing got %d", len(p.Files))
	}

	f := p.Files[0]
	f.Body = append(f.Body, t.generateTaskOption())

	return p, nil
}

// GenerateFluxASTReal is the real version of GenerateFluxAST. It has to exist so staticheck doesn't yell about
// the unexported functions I have here.
func (t Threshold) GenerateFluxASTReal() (*ast.Package, error) {
	p := parser.ParseSource(t.Query.Text)

	if errs := ast.GetErrors(p); len(errs) != 0 {
		return nil, multiError(errs)
	}

	// TODO(desa): this is a hack that we had to do as a result of https://github.com/influxdata/flux/issues/1701
	// when it is fixed we should use a separate file and not manipulate the existing one.
	if len(p.Files) != 1 {
		return nil, fmt.Errorf("expect a single file to be returned from query parsing got %d", len(p.Files))
	}

	f := p.Files[0]

	f.Imports = append(f.Imports, flux.Imports("influxdata/influxdb/alerts")...)
	f.Body = append(f.Body, t.generateFluxASTBody()...)

	return p, nil
}

func (t Threshold) generateTaskOption() ast.Statement {
	props := []*ast.Property{}

	props = append(props, flux.Property("name", flux.String(t.Name)))

	if t.Cron != "" {
		props = append(props, flux.Property("cron", flux.String(t.Cron)))
	}

	if t.Every != nil {
		props = append(props, flux.Property("every", (*ast.DurationLiteral)(t.Every)))
	}

	if t.Offset != nil {
		props = append(props, flux.Property("offset", (*ast.DurationLiteral)(t.Offset)))
	}

	return flux.DefineTaskOption(flux.Object(props...))
}

func (t Threshold) generateFluxASTBody() []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, t.generateTaskOption())
	statements = append(statements, t.generateFluxASTCheckDefinition())
	statements = append(statements, t.generateFluxASTThresholdFunctions()...)
	statements = append(statements, t.generateFluxASTMessageFunction())
	statements = append(statements, t.generateFluxASTChecksFunction())
	return statements
}

func (t Threshold) generateFluxASTMessageFunction() ast.Statement {
	fn := flux.Function(flux.FunctionParams("r", "check"), flux.String(t.StatusMessageTemplate))
	return flux.DefineVariable("messageFn", fn)
}

func (t Threshold) generateFluxASTChecksFunction() ast.Statement {
	return flux.ExpressionStatement(flux.Pipe(flux.Identifier("data"), t.generateFluxASTChecksCall()))
}

func (t Threshold) generateFluxASTChecksCall() *ast.CallExpression {
	objectProps := append(([]*ast.Property)(nil), flux.Property("check", flux.Identifier("check")))
	objectProps = append(objectProps, flux.Property("messageFn", flux.Identifier("messageFn")))

	// This assumes that the ThresholdConfigs we've been provided do not have duplicates.
	for _, c := range t.Thresholds {
		lvl := strings.ToLower(c.GetLevel().String())
		objectProps = append(objectProps, flux.Property(lvl, flux.Identifier(lvl)))
	}

	return flux.Call(flux.Member("alerts", "check"), flux.Object(objectProps...))
}

func (t Threshold) generateFluxASTCheckDefinition() ast.Statement {
	tagProperties := []*ast.Property{}
	for _, tag := range t.Tags {
		tagProperties = append(tagProperties, flux.Property(tag.Key, flux.String(tag.Value)))
	}
	tags := flux.Property("tags", flux.Object(tagProperties...))

	checkID := flux.Property("checkID", flux.String(t.ID.String()))

	return flux.DefineVariable("check", flux.Object(checkID, tags))
}

func (t Threshold) generateFluxASTThresholdFunctions() []ast.Statement {
	thresholdStatements := make([]ast.Statement, len(t.Thresholds))

	// This assumes that the ThresholdConfigs we've been provided do not have duplicates.
	for k, v := range t.Thresholds {
		thresholdStatements[k] = v.generateFluxASTThresholdFunction()
	}
	return thresholdStatements
}

func (td Greater) generateFluxASTThresholdFunction() ast.Statement {
	fnBody := flux.GreaterThan(flux.Member("r", "_value"), flux.Float(td.Value))
	fn := flux.Function(flux.FunctionParams("r"), fnBody)

	lvl := strings.ToLower(td.Level.String())

	return flux.DefineVariable(lvl, fn)
}

func (td Lesser) generateFluxASTThresholdFunction() ast.Statement {
	fnBody := flux.LessThan(flux.Member("r", "_value"), flux.Float(td.Value))
	fn := flux.Function(flux.FunctionParams("r"), fnBody)

	lvl := strings.ToLower(td.Level.String())

	return flux.DefineVariable(lvl, fn)
}

func (td Range) generateFluxASTThresholdFunction() ast.Statement {
	if !td.Within {
		td.Min, td.Max = td.Max, td.Min
	}
	fnBody := flux.And(
		flux.LessThan(flux.Member("r", "_value"), flux.Float(td.Max)),
		flux.GreaterThan(flux.Member("r", "_value"), flux.Float(td.Min)),
	)
	fn := flux.Function(flux.FunctionParams("r"), fnBody)

	lvl := strings.ToLower(td.Level.String())

	return flux.DefineVariable(lvl, fn)
}

type thresholdAlias Threshold

// MarshalJSON implement json.Marshaler interface.
func (t Threshold) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			thresholdAlias
			Type string `json:"type"`
		}{
			thresholdAlias: thresholdAlias(t),
			Type:           t.Type(),
		})
}

// ThresholdConfig is the base of all threshold config.
type ThresholdConfig interface {
	MarshalJSON() ([]byte, error)
	Valid() error
	Type() string
	generateFluxASTThresholdFunction() ast.Statement
	GetLevel() notification.CheckLevel
}

// Valid returns error if something is invalid.
func (b ThresholdConfigBase) Valid() error {
	return nil
}

// ThresholdConfigBase is the base of all threshold config.
type ThresholdConfigBase struct {
	// If true, only alert if all values meet threshold.
	AllValues bool                    `json:"allValues"`
	Level     notification.CheckLevel `json:"level"`
}

// GetLevel return the check level.
func (b ThresholdConfigBase) GetLevel() notification.CheckLevel {
	return b.Level
}

// Lesser threshold type.
type Lesser struct {
	ThresholdConfigBase
	Value float64 `json:"value,omitempty"`
}

// Type of the threshold config.
func (td Lesser) Type() string {
	return "lesser"
}

type lesserAlias Lesser

// MarshalJSON implement json.Marshaler interface.
func (td Lesser) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			lesserAlias
			Type string `json:"type"`
		}{
			lesserAlias: lesserAlias(td),
			Type:        "lesser",
		})
}

// Greater threshold type.
type Greater struct {
	ThresholdConfigBase
	Value float64 `json:"value,omitempty"`
}

// Type of the threshold config.
func (td Greater) Type() string {
	return "greater"
}

type greaterAlias Greater

// MarshalJSON implement json.Marshaler interface.
func (td Greater) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			greaterAlias
			Type string `json:"type"`
		}{
			greaterAlias: greaterAlias(td),
			Type:         "greater",
		})
}

// Range threshold type.
type Range struct {
	ThresholdConfigBase
	Min    float64 `json:"min,omitempty"`
	Max    float64 `json:"max,omitempty"`
	Within bool    `json:"within"`
}

// Type of the threshold config.
func (td Range) Type() string {
	return "range"
}

type rangeAlias Range

// MarshalJSON implement json.Marshaler interface.
func (td Range) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			rangeAlias
			Type string `json:"type"`
		}{
			rangeAlias: rangeAlias(td),
			Type:       "range",
		})
}

// Valid overwrite the base threshold.
func (td Range) Valid() error {
	if td.Min > td.Max {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "range threshold min can't be larger than max",
		}
	}
	return nil
}
