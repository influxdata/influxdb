package check

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/flux"
	"github.com/influxdata/influxdb/v2/query"
)

var _ influxdb.Check = (*Threshold)(nil)

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
func (t Threshold) Valid(lang influxdb.FluxLanguageService) error {
	if err := t.Base.Valid(lang); err != nil {
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
func (t Threshold) GenerateFlux(lang influxdb.FluxLanguageService) (string, error) {
	f, err := t.GenerateFluxAST(lang)
	if err != nil {
		return "", err
	}

	return astutil.Format(f)
}

// GenerateFluxAST returns a flux AST for the threshold provided. If there
// are any errors in the flux that the user provided the function will return
// an error for each error found when the script is parsed.
func (t Threshold) GenerateFluxAST(lang influxdb.FluxLanguageService) (*ast.File, error) {
	p, err := query.Parse(lang, t.Query.Text)
	if p == nil {
		return nil, err
	}
	replaceDurationsWithEvery(p, t.Every)
	removeStopFromRange(p)
	addCreateEmptyFalseToAggregateWindow(p)

	if errs := ast.GetErrors(p); len(errs) != 0 {
		return nil, multiError(errs)
	}

	// TODO(desa): this is a hack that we had to do as a result of https://github.com/influxdata/flux/issues/1701
	// when it is fixed we should use a separate file and not manipulate the existing one.
	if len(p.Files) != 1 {
		return nil, fmt.Errorf("expect a single file to be returned from query parsing got %d", len(p.Files))
	}

	fields := getFields(p)
	if len(fields) != 1 {
		return nil, fmt.Errorf("expected a single field but got: %s", fields)
	}

	f := p.Files[0]
	assignPipelineToData(f)

	f.Imports = append(f.Imports, flux.Imports("influxdata/influxdb/monitor", "influxdata/influxdb/v1")...)
	f.Body = append(f.Body, t.generateFluxASTBody(fields[0])...)

	return f, nil
}

// TODO(desa): we'll likely want something slightly more sophisitcated long term, but this should work for now.
func addCreateEmptyFalseToAggregateWindow(pkg *ast.Package) {
	ast.Visit(pkg, func(n ast.Node) {
		if call, ok := n.(*ast.CallExpression); ok {
			if id, ok := call.Callee.(*ast.Identifier); ok && id.Name == "aggregateWindow" {
				for _, args := range call.Arguments {
					if obj, ok := args.(*ast.ObjectExpression); ok {
						foundCreateEmpty := false
						for _, props := range obj.Properties {
							if props.Key.Key() == "createEmpty" {
								foundCreateEmpty = true
								break
							}
						}
						if !foundCreateEmpty {
							obj.Properties = append(obj.Properties, flux.Property("createEmpty", flux.Bool(false)))
						}
					}
				}
			}
		}
	})
}

// TODO(desa): we'll likely want something slightly more sophisitcated long term, but this should work for now.
func replaceDurationsWithEvery(pkg *ast.Package, every *notification.Duration) {
	ast.Visit(pkg, func(n ast.Node) {
		switch e := n.(type) {
		case *ast.Property:
			key := e.Key.Key()
			newEvery := (ast.DurationLiteral)(*every)
			switch key {
			case "start":
				e.Value = flux.Negative(&newEvery)
			case "every":
				e.Value = &newEvery
			}
		}
	})
}

// TODO(desa): we'll likely want to remove all other arguments to range that are provided, but for now this should work.
// When we decide to implement the full feature we'll have to do something more sophisticated.
func removeStopFromRange(pkg *ast.Package) {
	ast.Visit(pkg, func(n ast.Node) {
		if call, ok := n.(*ast.CallExpression); ok {
			if id, ok := call.Callee.(*ast.Identifier); ok && id.Name == "range" {
				for _, args := range call.Arguments {
					if obj, ok := args.(*ast.ObjectExpression); ok {
						props := obj.Properties[:0]
						for _, prop := range obj.Properties {
							if prop.Key.Key() == "start" {
								props = append(props, prop)
							}
						}
						obj.Properties = props
					}
				}
			}
		}
	})
}

// TODO(desa): we'll likely want to remove all other arguments to range that are provided, but for now this should work.
// When we decide to implement the full feature we'll have to do something more sophisticated.
func removeAggregateWindow(pkg *ast.Package) {
	ast.Visit(pkg, func(n ast.Node) {
		if pipe, ok := n.(*ast.PipeExpression); ok {
			if id, ok := pipe.Call.Callee.(*ast.Identifier); ok && id.Name == "aggregateWindow" {
				if subPipe, ok := pipe.Argument.(*ast.PipeExpression); ok {
					*pipe = *subPipe
				}
			}
		}
	})
}

func getFields(pkg *ast.Package) []string {
	var fields []string
	ast.Visit(pkg, func(n ast.Node) {
		if fn, ok := n.(*ast.BinaryExpression); ok {
			if me, ok := fn.Left.(*ast.MemberExpression); ok {
				if me.Property.Key() == "_field" {
					if str, ok := fn.Right.(*ast.StringLiteral); ok {
						fields = append(fields, str.Value)
					}
				}
			}
		}
	})
	return fields
}

func assignPipelineToData(f *ast.File) error {
	if len(f.Body) != 1 {
		return fmt.Errorf("expected there to be a single statement in the flux script body, received %d", len(f.Body))
	}

	stmt := f.Body[0]

	e, ok := stmt.(*ast.ExpressionStatement)
	if !ok {
		return fmt.Errorf("statement is not an *ast.Expression statement, received %T", stmt)
	}

	exp := e.Expression

	pipe, ok := exp.(*ast.PipeExpression)
	if !ok {
		return fmt.Errorf("expression is not an *ast.PipeExpression statement, received %T", exp)
	}

	if id, ok := pipe.Call.Callee.(*ast.Identifier); ok && id.Name == "yield" {
		exp = pipe.Argument
	}

	f.Body[0] = flux.DefineVariable("data", exp)
	return nil
}

func (t Threshold) generateFluxASTBody(field string) []ast.Statement {
	var statements []ast.Statement
	statements = append(statements, t.generateTaskOption())
	statements = append(statements, t.generateFluxASTCheckDefinition("threshold"))
	statements = append(statements, t.generateFluxASTThresholdFunctions(field)...)
	statements = append(statements, t.generateFluxASTMessageFunction())
	statements = append(statements, t.generateFluxASTChecksFunction())
	return statements
}

func (t Threshold) generateFluxASTChecksFunction() ast.Statement {
	return flux.ExpressionStatement(flux.Pipe(
		flux.Identifier("data"),
		flux.Call(flux.Member("v1", "fieldsAsCols"), flux.Object()),
		t.generateFluxASTChecksCall(),
	))
}

func (t Threshold) generateFluxASTChecksCall() *ast.CallExpression {
	objectProps := append(([]*ast.Property)(nil), flux.Property("data", flux.Identifier("check")))
	objectProps = append(objectProps, flux.Property("messageFn", flux.Identifier("messageFn")))

	// This assumes that the ThresholdConfigs we've been provided do not have duplicates.
	for _, c := range t.Thresholds {
		lvl := strings.ToLower(c.GetLevel().String())
		objectProps = append(objectProps, flux.Property(lvl, flux.Identifier(lvl)))
	}

	return flux.Call(flux.Member("monitor", "check"), flux.Object(objectProps...))
}

func (t Threshold) generateFluxASTThresholdFunctions(field string) []ast.Statement {
	thresholdStatements := make([]ast.Statement, len(t.Thresholds))

	// This assumes that the ThresholdConfigs we've been provided do not have duplicates.
	for k, v := range t.Thresholds {
		thresholdStatements[k] = v.generateFluxASTThresholdFunction(field)
	}
	return thresholdStatements
}

func (td Greater) generateFluxASTThresholdFunction(field string) ast.Statement {
	fnBody := flux.GreaterThan(flux.Member("r", field), flux.Float(td.Value))
	fn := flux.Function(flux.FunctionParams("r"), fnBody)

	lvl := strings.ToLower(td.Level.String())

	return flux.DefineVariable(lvl, fn)
}

func (td Lesser) generateFluxASTThresholdFunction(field string) ast.Statement {
	fnBody := flux.LessThan(flux.Member("r", field), flux.Float(td.Value))
	fn := flux.Function(flux.FunctionParams("r"), fnBody)

	lvl := strings.ToLower(td.Level.String())

	return flux.DefineVariable(lvl, fn)
}

func (td Range) generateFluxASTThresholdFunction(field string) ast.Statement {
	var fnBody *ast.LogicalExpression
	if !td.Within {
		fnBody = flux.Or(
			flux.LessThan(flux.Member("r", field), flux.Float(td.Min)),
			flux.GreaterThan(flux.Member("r", field), flux.Float(td.Max)),
		)
	} else {
		fnBody = flux.And(
			flux.LessThan(flux.Member("r", field), flux.Float(td.Max)),
			flux.GreaterThan(flux.Member("r", field), flux.Float(td.Min)),
		)
	}

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
	generateFluxASTThresholdFunction(string) ast.Statement
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
	Value float64 `json:"value"`
}

// Type of the threshold config.
func (td Lesser) Type() string {
	return "lesser"
}

// MarshalJSON implement json.Marshaler interface.
func (td Lesser) MarshalJSON() ([]byte, error) {
	type lesserAlias Lesser
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
	Value float64 `json:"value"`
}

// Type of the threshold config.
func (td Greater) Type() string {
	return "greater"
}

// MarshalJSON implement json.Marshaler interface.
func (td Greater) MarshalJSON() ([]byte, error) {
	type greaterAlias Greater
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

// MarshalJSON implement json.Marshaler interface.
func (td Range) MarshalJSON() ([]byte, error) {
	type rangeAlias Range
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
