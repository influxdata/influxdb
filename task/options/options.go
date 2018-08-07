// Package options provides ways to extract the task-related options from a Flux script.
package options

import (
	"errors"
	"sync"
	"time"

	"github.com/influxdata/platform/query"
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
		concurrency := concurrencyVal.Int()
		if concurrency > maxConcurrency {
			return opt, errors.New("concurrency exceeded max concurrency")
		}
		if concurrency < 1 {
			return opt, errors.New("atleast 1 concurrency required")
		}
		opt.Concurrency = concurrency
	}

	if retryVal, ok := optObject.Get("retry"); ok {
		retry := retryVal.Int()
		if retry > maxRetry {
			return opt, errors.New("retry exceeded max retry")
		}
		if retry < 1 {
			return opt, errors.New("atleast 1 retry required")
		}
		opt.Retry = retry
	}

	if optionCache != nil {
		optionCacheMu.Lock()
		optionCache[script] = opt
		optionCacheMu.Unlock()
	}

	return opt, nil
}
