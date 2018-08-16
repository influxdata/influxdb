// Package options provides ways to extract the task-related options from a Flux script.
package options

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/platform/query"
	cron "gopkg.in/robfig/cron.v2"
)

// optionCache is enabled for tests, to work around https://github.com/influxdata/platform/issues/484.
var optionCache map[string]Options

var optionCacheMu sync.Mutex

// EnableScriptCacheForTest is used as a workaround for https://github.com/influxdata/platform/issues/484,
// and should be removed after that issue is addressed.
// Do not call this method in production code, as it will leak memory.
func EnableScriptCacheForTest() {
	optionCache = make(map[string]Options)
}

const maxConcurrency = 100
const maxRetry = 10

// Options are the task-related options that can be specified in a Flux script.
type Options struct {
	// Name is a non optional name designator for each task.
	Name string

	// Cron is a cron style time schedule that can be used in place of Every.
	Cron string

	// Every represents a fixed period to repeat execution.
	Every time.Duration

	// Delay represents a delay before execution.
	Delay time.Duration

	Concurrency int64

	Retry int64
}

// FromScript extracts Options from a Flux script.
func FromScript(script string) (Options, error) {
	if optionCache != nil {
		optionCacheMu.Lock()
		opt, ok := optionCache[script]
		optionCacheMu.Unlock()

		if ok {
			return opt, nil
		}
	}

	opt := Options{Retry: 1, Concurrency: 1}

	inter := query.NewInterpreter()
	if err := query.Eval(inter, script); err != nil {
		return opt, err
	}

	// pull options from interpreter
	task := inter.Option("task")
	if task == nil {
		return opt, errors.New("task not defined")
	}
	optObject := task.Object()

	nameVal, ok := optObject.Get("name")
	if !ok {
		return opt, errors.New("missing name in task options")
	}
	opt.Name = nameVal.Str()

	crVal, cronOK := optObject.Get("cron")
	everyVal, everyOK := optObject.Get("every")
	if cronOK && everyOK {
		return opt, errors.New("cannot use both cron and every in task options")
	}
	if !cronOK && !everyOK {
		return opt, errors.New("cron or every is required")
	}

	if cronOK {
		opt.Cron = crVal.Str()
	}
	if everyOK {
		opt.Every = everyVal.Duration().Duration()
	}

	if delayVal, ok := optObject.Get("delay"); ok {
		opt.Delay = delayVal.Duration().Duration()
	}

	if concurrencyVal, ok := optObject.Get("concurrency"); ok {
		opt.Concurrency = concurrencyVal.Int()
	}

	if retryVal, ok := optObject.Get("retry"); ok {
		opt.Retry = retryVal.Int()
	}

	if err := opt.Validate(); err != nil {
		return opt, err
	}

	if optionCache != nil {
		optionCacheMu.Lock()
		optionCache[script] = opt
		optionCacheMu.Unlock()
	}

	return opt, nil
}

// Validate returns an error if the options aren't valid.
func (o *Options) Validate() error {
	var errs []string
	if o.Name == "" {
		errs = append(errs, "name required")
	}

	cronPresent := o.Cron != ""
	everyPresent := o.Every != 0
	if cronPresent == everyPresent {
		// They're both present or both missing.
		errs = append(errs, "must specify exactly one of either cron or every")
	} else if cronPresent {
		_, err := cron.Parse(o.Cron)
		if err != nil {
			errs = append(errs, "cron invalid: "+err.Error())
		}
	} else if everyPresent {
		if o.Every < time.Second {
			errs = append(errs, "every option must be at least 1 second")
		} else if o.Every.Truncate(time.Second) != o.Every {
			errs = append(errs, "every option must be expressible as whole seconds")
		}
	}

	if o.Delay.Truncate(time.Second) != o.Delay {
		// For now, allowing negative delays. Maybe they're useful for forecasting?
		errs = append(errs, "delay option must be expressible as whole seconds")
	}

	if o.Concurrency < 1 {
		errs = append(errs, "concurrency must be at least 1")
	} else if o.Concurrency > maxConcurrency {
		errs = append(errs, fmt.Sprintf("concurrency exceeded max of %d", maxConcurrency))
	}

	if o.Retry < 1 {
		errs = append(errs, "retry must be at least 1")
	} else if o.Retry > maxRetry {
		errs = append(errs, fmt.Sprintf("retry exceeded max of %d", maxRetry))
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
// The value of the delay option is not considered.
func (o *Options) EffectiveCronString() string {
	if o.Cron != "" {
		return o.Cron
	}
	if o.Every > 0 {
		return "@every " + o.Every.String()
	}
	return ""
}
