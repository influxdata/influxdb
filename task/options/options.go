// Package options provides ways to extract the task-related options from a Flux script.
package options

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/cron"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/semantic"
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
	q, err := parseSignedDuration(s)
	if err != nil {
		return ErrTaskInvalidDuration(err)
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

// parseSignedDuration is a helper wrapper around parser.ParseSignedDuration.
// We use it because we need to clear the basenode, but flux does not.
func parseSignedDuration(text string) (*ast.DurationLiteral, error) {
	q, err := parser.ParseSignedDuration(text)
	if err != nil {
		return nil, err
	}
	q.BaseNode = ast.BaseNode{}
	return q, err
}

// UnmarshalText unmarshals text into a Duration.
func (a *Duration) UnmarshalText(text []byte) error {
	q, err := parseSignedDuration(string(text))
	if err != nil {
		return err
	}
	a.Node = *q
	return nil
}

// UnmarshalText marshals text into a Duration.
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

// contains is a helper function to see if an array of strings contains a string
func contains(s []string, e string) bool {
	for i := range s {
		if s[i] == e {
			return true
		}
	}
	return false
}

func grabTaskOptionAST(p *ast.Package, keys ...string) map[string]ast.Expression {
	res := make(map[string]ast.Expression, 2) // we preallocate two keys for the map, as that is how many we will use at maximum (offset and every)
	for i := range p.Files {
		for j := range p.Files[i].Body {
			if p.Files[i].Body[j].Type() != "OptionStatement" {
				continue
			}
			opt := (p.Files[i].Body[j]).(*ast.OptionStatement)
			if opt.Assignment.Type() != "VariableAssignment" {
				continue
			}
			asmt, ok := opt.Assignment.(*ast.VariableAssignment)
			if !ok {
				continue
			}
			if asmt.ID.Key() != "task" {
				continue
			}
			ae, ok := asmt.Init.(*ast.ObjectExpression)
			if !ok {
				continue
			}
			for k := range ae.Properties {
				prop := ae.Properties[k]
				if key := prop.Key.Key(); prop != nil && contains(keys, key) {
					res[key] = prop.Value
				}
			}
			return res
		}
	}
	return res
}

func newDeps() flux.Dependencies {
	deps := flux.NewDefaultDependencies()
	deps.Deps.HTTPClient = httpClient{}
	deps.Deps.URLValidator = urlValidator{}
	deps.Deps.SecretService = secretService{}
	deps.Deps.FilesystemService = fileSystem{}
	return deps
}

// FromScript extracts Options from a Flux script.
func FromScript(script string) (Options, error) {
	opt := Options{Retry: pointer.Int64(1), Concurrency: pointer.Int64(1)}

	fluxAST, err := flux.Parse(script)
	if err != nil {
		return opt, err
	}
	durTypes := grabTaskOptionAST(fluxAST, optEvery, optOffset)
	// TODO(desa): should be dependencies.NewEmpty(), but for now we'll hack things together
	ctx := newDeps().Inject(context.Background())
	_, scope, err := flux.EvalAST(ctx, fluxAST)
	if err != nil {
		return opt, err
	}

	// pull options from the program scope
	task, ok := scope.Lookup("task")
	if !ok {
		return opt, ErrMissingRequiredTaskOption("task")
	}
	// check to make sure task is an object
	if err := checkNature(task.PolyType().Nature(), semantic.Object); err != nil {
		return opt, err
	}
	optObject := task.Object()
	if err := validateOptionNames(optObject); err != nil {
		return opt, err
	}

	nameVal, ok := optObject.Get(optName)
	if !ok {
		return opt, ErrMissingRequiredTaskOption("name")
	}

	if err := checkNature(nameVal.PolyType().Nature(), semantic.String); err != nil {
		return opt, err
	}
	opt.Name = nameVal.Str()
	crVal, cronOK := optObject.Get(optCron)
	everyVal, everyOK := optObject.Get(optEvery)
	if cronOK && everyOK {
		return opt, ErrDuplicateIntervalField
	}

	if !cronOK && !everyOK {
		return opt, ErrMissingRequiredTaskOption("cron or every is required")
	}

	if cronOK {
		if err := checkNature(crVal.PolyType().Nature(), semantic.String); err != nil {
			return opt, err
		}
		opt.Cron = crVal.Str()
	}

	if everyOK {
		if err := checkNature(everyVal.PolyType().Nature(), semantic.Duration); err != nil {
			return opt, err
		}
		dur, ok := durTypes["every"]
		if !ok || dur == nil {
			return opt, ErrParseTaskOptionField("every")
		}
		durNode, err := parseSignedDuration(dur.Location().Source)
		if err != nil {
			return opt, err
		}

		if !ok || durNode == nil {
			return opt, ErrParseTaskOptionField("every")
		}

		durNode.BaseNode = ast.BaseNode{}
		opt.Every.Node = *durNode
	}

	if offsetVal, ok := optObject.Get(optOffset); ok {
		if err := checkNature(offsetVal.PolyType().Nature(), semantic.Duration); err != nil {
			return opt, err
		}
		dur, ok := durTypes["offset"]
		if !ok || dur == nil {
			return opt, ErrParseTaskOptionField("offset")
		}
		durNode, err := parseSignedDuration(dur.Location().Source)
		if err != nil {
			return opt, err
		}
		if !ok || durNode == nil {
			return opt, ErrParseTaskOptionField("offset")
		}
		durNode.BaseNode = ast.BaseNode{}
		opt.Offset = &Duration{}
		opt.Offset.Node = *durNode
	}

	if concurrencyVal, ok := optObject.Get(optConcurrency); ok {
		if err := checkNature(concurrencyVal.PolyType().Nature(), semantic.Int); err != nil {
			return opt, err
		}
		opt.Concurrency = pointer.Int64(concurrencyVal.Int())
	}

	if retryVal, ok := optObject.Get(optRetry); ok {
		if err := checkNature(retryVal.PolyType().Nature(), semantic.Int); err != nil {
			return opt, err
		}
		opt.Retry = pointer.Int64(retryVal.Int())
	}

	if err := opt.Validate(); err != nil {
		return opt, err
	}

	return opt, nil
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
	every, _ := o.Every.DurationFrom(time.Now()) // we can ignore errors here because we have alreach checked for validity.
	if every > 0 {
		return "@every " + o.Every.String()
	}
	return ""
}

// checkNature returns a clean error of got and expected dont match.
func checkNature(got, exp semantic.Nature) error {
	if got != exp {
		return fmt.Errorf("unexpected kind: got %q expected %q", got, exp)
	}
	return nil
}

// validateOptionNames returns an error if any keys in the option object o
// do not match an expected option name.
func validateOptionNames(o values.Object) error {
	var unexpected []string
	o.Range(func(name string, _ values.Value) {
		switch name {
		case optName, optCron, optEvery, optOffset, optConcurrency, optRetry:
			// Known option. Nothing to do.
		default:
			unexpected = append(unexpected, name)
		}
	})

	if len(unexpected) > 0 {
		u := strings.Join(unexpected, ", ")
		v := strings.Join([]string{optName, optCron, optEvery, optOffset, optConcurrency, optRetry}, ", ")
		return fmt.Errorf("unknown task option(s): %s. valid options are %s", u, v)
	}

	return nil
}
