// Package options provides ways to extract the task-related options from a Flux script.
package options

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/cron"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/edit"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
)

const maxConcurrency = 100
const maxRetry = 10

// Options are the task-related options that can be specified in a Flux script.
type Options struct {
	// Name is a non optional name designator for each task.
	Name string `json:"name,omitempty"`

	// Cron is a cron style time schedule that can be used in place of Every.
	Cron string `json:"cron,omitempty"`

	// Every represents a fixed period to repeat execution.
	// this can be unmarshaled from json as a string i.e.: "1d" will unmarshal as 1 day
	Every Duration `json:"every,omitempty"`

	// Offset represents a delay before execution.
	// this can be unmarshaled from json as a string i.e.: "1d" will unmarshal as 1 day
	Offset *Duration `json:"offset,omitempty"`

	Concurrency *int64 `json:"concurrency,omitempty"`

	Retry *int64 `json:"retry,omitempty"`
}

// Duration is a time span that supports the same units as the flux parser's time duration, as well as negative length time spans.
type Duration struct {
	Node ast.DurationLiteral
}

func (a Duration) String() string {
	return ast.Format(&a.Node)
}

// Parse parses a string into a Duration.
func (a *Duration) Parse(s string) error {
	q, err := ParseSignedDuration(s)
	if err != nil {
		return errTaskInvalidDuration(err)
	}
	a.Node = *q
	return nil
}

// MustParseDuration parses a string and returns a duration.  It panics if there is an error.
func MustParseDuration(s string) (dur *Duration) {
	dur = &Duration{}
	if err := dur.Parse(s); err != nil {
		panic(err)
	}
	return dur
}

// UnmarshalText unmarshals text into a Duration.
func (a *Duration) UnmarshalText(text []byte) error {
	q, err := ParseSignedDuration(string(text))
	if err != nil {
		return err
	}
	a.Node = *q
	return nil
}

// MarshalText marshals text into a Duration.
func (a Duration) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// IsZero checks if each segment of the duration is zero, it doesn't check if the Duration sums to zero, just if each internal duration is zero.
func (a *Duration) IsZero() bool {
	for i := range a.Node.Values {
		if a.Node.Values[i].Magnitude != 0 {
			return false
		}
	}
	return true
}

// DurationFrom gives us a time.Duration from a time.
// Currently because of how flux works, this is just an approfimation for any time unit larger than hours.
func (a *Duration) DurationFrom(t time.Time) (time.Duration, error) {
	return ast.DurationFrom(&a.Node, t)
}

// Add adds the duration to a time.
func (a *Duration) Add(t time.Time) (time.Time, error) {
	d, err := ast.DurationFrom(&a.Node, t)
	if err != nil {
		return time.Time{}, err
	}
	return t.Add(d), nil
}

// Clear clears out all options in the options struct, it us useful if you wish to reuse it.
func (o *Options) Clear() {
	o.Name = ""
	o.Cron = ""
	o.Every = Duration{}
	o.Offset = nil
	o.Concurrency = nil
	o.Retry = nil
}

// IsZero tells us if the options has been zeroed out.
func (o *Options) IsZero() bool {
	return o.Name == "" &&
		o.Cron == "" &&
		o.Every.IsZero() &&
		(o.Offset == nil || o.Offset.IsZero()) &&
		o.Concurrency == nil &&
		o.Retry == nil
}

// All the task option names we accept.
const (
	optName        = "name"
	optCron        = "cron"
	optEvery       = "every"
	optOffset      = "offset"
	optConcurrency = "concurrency"
	optRetry       = "retry"
)

// FluxLanguageService is a service for interacting with flux code.
type FluxLanguageService interface {
	// Parse will take flux source code and produce a package.
	// If there are errors when parsing, the first error is returned.
	// An ast.Package may be returned when a parsing error occurs,
	// but it may be null if parsing didn't even occur.
	Parse(source string) (*ast.Package, error)

	// EvalAST will evaluate and run an AST.
	EvalAST(ctx context.Context, astPkg *ast.Package) ([]interpreter.SideEffect, values.Scope, error)
}

// FromScriptAST extracts Task options from a Flux script using only the AST (no
// evaluation of the script). Using AST here allows us to avoid having to
// contend with functions that aren't available in some parsing contexts (within
// Gateway for example).
func FromScriptAST(lang FluxLanguageService, script string) (Options, error) {
	opts := Options{
		Retry:       pointer.Int64(1),
		Concurrency: pointer.Int64(1),
	}

	fluxAST, err := parse(lang, script)
	if err != nil {
		return opts, err
	}

	if len(fluxAST.Files) == 0 {
		return opts, ErrNoASTFile
	}

	file := fluxAST.Files[0]
	if hasDuplicateOptions(file, "task") {
		return opts, ErrMultipleTaskOptionsDefined
	}

	obj, err := edit.GetOption(file, "task")
	if err != nil {
		return opts, ErrNoTaskOptionsDefined
	}

	objExpr, ok := obj.(*ast.ObjectExpression)
	if !ok {
		return opts, errTaskOptionNotObjectExpression(objExpr.Type())
	}

	for _, fn := range taskOptionExtractors {
		if err := fn(&opts, objExpr); err != nil {
			return opts, err
		}
	}

	if err := opts.Validate(); err != nil {
		return opts, err
	}

	return opts, nil
}

// hasDuplicateOptions determines whether or not there are multiple assignments
// to the same option variable.
//
// TODO(brett): This will be superceded by edit.HasDuplicateOptions once its available.
func hasDuplicateOptions(file *ast.File, name string) bool {
	var n int
	for _, st := range file.Body {
		if val, ok := st.(*ast.OptionStatement); ok {
			assign := val.Assignment
			if va, ok := assign.(*ast.VariableAssignment); ok {
				if va.ID.Name == name {
					n++
				}
			}
		}
	}
	return n > 1
}

type extractFn func(*Options, *ast.ObjectExpression) error

var taskOptionExtractors = []extractFn{
	extractNameOption,
	extractScheduleOptions,
	extractOffsetOption,
	extractConcurrencyOption,
	extractRetryOption,
}

func extractNameOption(opts *Options, objExpr *ast.ObjectExpression) error {
	nameExpr, err := edit.GetProperty(objExpr, optName)
	if err != nil {
		return errMissingRequiredTaskOption(optName)
	}
	nameStr, ok := nameExpr.(*ast.StringLiteral)
	if !ok {
		return errParseTaskOptionField(optName)
	}
	opts.Name = ast.StringFromLiteral(nameStr)

	return nil
}

func extractScheduleOptions(opts *Options, objExpr *ast.ObjectExpression) error {
	cronExpr, cronErr := edit.GetProperty(objExpr, optCron)
	everyExpr, everyErr := edit.GetProperty(objExpr, optEvery)
	if cronErr == nil && everyErr == nil {
		return ErrDuplicateIntervalField
	}
	if cronErr != nil && everyErr != nil {
		return errMissingRequiredTaskOption("cron or every")
	}

	if cronErr == nil {
		cronExprStr, ok := cronExpr.(*ast.StringLiteral)
		if !ok {
			return errParseTaskOptionField(optCron)
		}
		opts.Cron = ast.StringFromLiteral(cronExprStr)
	}

	if everyErr == nil {
		everyDur, ok := everyExpr.(*ast.DurationLiteral)
		if !ok {
			return errParseTaskOptionField(optEvery)
		}
		opts.Every = Duration{Node: *everyDur}
	}

	return nil
}

func extractOffsetOption(opts *Options, objExpr *ast.ObjectExpression) error {
	offsetExpr, offsetErr := edit.GetProperty(objExpr, optOffset)
	if offsetErr != nil {
		return nil
	}

	switch offsetExprV := offsetExpr.(type) {
	case *ast.UnaryExpression:
		offsetDur, err := ParseSignedDuration(offsetExprV.Loc.Source)
		if err != nil {
			return err
		}
		opts.Offset = &Duration{Node: *offsetDur}
	case *ast.DurationLiteral:
		opts.Offset = &Duration{Node: *offsetExprV}
	default:
		return errParseTaskOptionField(optOffset)
	}

	return nil
}

func extractConcurrencyOption(opts *Options, objExpr *ast.ObjectExpression) error {
	concurExpr, err := edit.GetProperty(objExpr, optConcurrency)
	if err != nil {
		return nil
	}

	concurInt, ok := concurExpr.(*ast.IntegerLiteral)
	if !ok {
		return errParseTaskOptionField(optConcurrency)
	}
	val := ast.IntegerFromLiteral(concurInt)
	opts.Concurrency = &val

	return nil
}

func extractRetryOption(opts *Options, objExpr *ast.ObjectExpression) error {
	retryExpr, err := edit.GetProperty(objExpr, optRetry)
	if err != nil {
		return nil
	}

	retryInt, ok := retryExpr.(*ast.IntegerLiteral)
	if !ok {
		return errParseTaskOptionField(optRetry)
	}
	val := ast.IntegerFromLiteral(retryInt)
	opts.Retry = &val

	return nil
}

// Validate returns an error if the options aren't valid.
func (o *Options) Validate() error {
	now := time.Now()
	var errs []string
	if o.Name == "" {
		errs = append(errs, "name required")
	}

	cronPresent := o.Cron != ""
	everyPresent := !o.Every.IsZero()
	if cronPresent == everyPresent {
		// They're both present or both missing.
		errs = append(errs, "must specify exactly one of either cron or every")
	} else if cronPresent {
		_, err := cron.ParseUTC(o.Cron)
		if err != nil {
			errs = append(errs, "cron invalid: "+err.Error())
		}
	} else if everyPresent {
		every, err := o.Every.DurationFrom(now)
		if err != nil {
			return err
		}
		if every < time.Second {
			errs = append(errs, "every option must be at least 1 second")
		} else if every.Truncate(time.Second) != every {
			errs = append(errs, "every option must be expressible as whole seconds")
		}
	}
	if o.Offset != nil {
		offset, err := o.Offset.DurationFrom(now)
		if err != nil {
			return err
		}
		if offset.Truncate(time.Second) != offset {
			// For now, allowing negative offset delays. Maybe they're useful for forecasting?
			errs = append(errs, "offset option must be expressible as whole seconds")
		}
	}
	if o.Concurrency != nil {
		if *o.Concurrency < 1 {
			errs = append(errs, "concurrency must be at least 1")
		} else if *o.Concurrency > maxConcurrency {
			errs = append(errs, fmt.Sprintf("concurrency exceeded max of %d", maxConcurrency))
		}
	}
	if o.Retry != nil {
		if *o.Retry < 1 {
			errs = append(errs, "retry must be at least 1")
		} else if *o.Retry > maxRetry {
			errs = append(errs, fmt.Sprintf("retry exceeded max of %d", maxRetry))
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("invalid options: %s", strings.Join(errs, ", "))
}

// EffectiveCronString returns the effective cron string of the options.
// If the cron option was specified, it is returned.
// If the every option was specified, it is converted into a cron string using "@every".
// Otherwise, the empty string is returned.
// The value of the offset option is not considered.
// TODO(docmerlin): create an EffectiveCronStringFrom(t time.Time) string,
// that works from a unit of time.
// Do not use this if you haven't checked for validity already.
func (o *Options) EffectiveCronString() string {
	if o.Cron != "" {
		return o.Cron
	}
	every, _ := o.Every.DurationFrom(time.Now()) // we can ignore errors here because we have already checked for validity.
	if every > 0 {
		return "@every " + o.Every.String()
	}
	return ""
}

// parse will take flux source code and produce a package.
// If there are errors when parsing, the first error is returned.
// An ast.Package may be returned when a parsing error occurs,
// but it may be null if parsing didn't even occur.
//
// This will return an error if the FluxLanguageService is nil.
func parse(lang FluxLanguageService, source string) (*ast.Package, error) {
	if lang == nil {
		return nil, errors.New("flux is not configured; cannot parse")
	}
	return lang.Parse(source)
}
