package options_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/task/options"
)

func scriptGenerator(opt options.Options, body string) string {
	taskData := ""
	if opt.Name != "" {
		taskData = fmt.Sprintf("%s  name: %q,\n", taskData, opt.Name)
	}
	if opt.Cron != "" {
		taskData = fmt.Sprintf("%s  cron: %q,\n", taskData, opt.Cron)
	}
	if opt.Every != 0 {
		taskData = fmt.Sprintf("%s  every: %s,\n", taskData, opt.Every.String())
	}
	if opt.Delay != 0 {
		taskData = fmt.Sprintf("%s  delay: %s,\n", taskData, opt.Delay.String())
	}
	if opt.Concurrency != 0 {
		taskData = fmt.Sprintf("%s  concurrency: %d,\n", taskData, opt.Concurrency)
	}
	if opt.Retry != 0 {
		taskData = fmt.Sprintf("%s  retry: %d,\n", taskData, opt.Retry)
	}
	if body == "" {
		body = `from(bucket: "test")
    |> range(start:-1h)`
	}

	return fmt.Sprintf(`option task = {
%s
}

%s`, taskData, body)
}

func TestFromScript(t *testing.T) {
	for _, c := range []struct {
		script    string
		exp       options.Options
		shouldErr bool
	}{
		{script: scriptGenerator(options.Options{Name: "name", Cron: "* * * * *", Concurrency: 2, Retry: 3, Delay: -time.Minute}, ""), exp: options.Options{Name: "name", Cron: "* * * * *", Concurrency: 2, Retry: 3, Delay: -time.Minute}},
		{script: scriptGenerator(options.Options{Name: "name", Every: 5 * time.Second}, ""), exp: options.Options{Name: "name", Every: 5 * time.Second, Concurrency: 1, Retry: 1}},
		{script: scriptGenerator(options.Options{Name: "name", Cron: "* * * * *"}, ""), exp: options.Options{Name: "name", Cron: "* * * * *", Concurrency: 1, Retry: 1}},
		{script: scriptGenerator(options.Options{Name: "name", Every: time.Hour, Cron: "* * * * *"}, ""), shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name", Concurrency: 1000, Every: time.Hour}, ""), shouldErr: true},
		{script: "option task = {\n  name: \"name\",\n  concurrency: 0,\n  every: 1m0s,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: "option task = {\n  name: \"name\",\n  concurrency: 1,\n  every: 1,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name", Retry: 20, Every: time.Hour}, ""), shouldErr: true},
		{script: "option task = {\n  name: \"name\",\n  retry: 0,\n  every: 1m0s,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name"}, ""), shouldErr: true},
		{script: scriptGenerator(options.Options{}, ""), shouldErr: true},
	} {
		o, err := options.FromScript(c.script)
		if c.shouldErr && err == nil {
			t.Fatalf("script %q should have errored but didn't", c.script)
		} else if !c.shouldErr && err != nil {
			t.Fatalf("script %q should not have errored, but got %v", c.script, err)
		}

		if err != nil {
			continue
		}
		if !cmp.Equal(o, c.exp) {
			t.Fatalf("script %q got unexpected result -got/+exp\n%s", c.script, cmp.Diff(o, c.exp))
		}
	}
}

func TestValidate(t *testing.T) {
	good := options.Options{Name: "x", Cron: "* * * * *", Concurrency: 1, Retry: 1}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}

	bad := new(options.Options)
	*bad = good
	bad.Name = ""
	if err := bad.Validate(); err == nil {
		t.Error("expected error for options without name")
	}

	*bad = good
	bad.Cron = ""
	if err := bad.Validate(); err == nil {
		t.Error("expected error for options without cron or every")
	}

	*bad = good
	bad.Every = time.Minute
	if err := bad.Validate(); err == nil {
		t.Error("expected error for options with both cron and every")
	}

	*bad = good
	bad.Cron = "not a cron string"
	if err := bad.Validate(); err == nil {
		t.Error("expected error for options with invalid cron")
	}

	*bad = good
	bad.Cron = ""
	bad.Every = -1 * time.Minute
	if err := bad.Validate(); err == nil {
		t.Error("expected error for negative every")
	}

	*bad = good
	bad.Delay = 1500 * time.Millisecond
	if err := bad.Validate(); err == nil {
		t.Error("expected error for sub-second delay resolution")
	}

	*bad = good
	bad.Concurrency = 0
	if err := bad.Validate(); err == nil {
		t.Error("expected error for 0 concurrency")
	}

	*bad = good
	bad.Concurrency = math.MaxInt64
	if err := bad.Validate(); err == nil {
		t.Error("expected error for concurrency too large")
	}

	*bad = good
	bad.Retry = 0
	if err := bad.Validate(); err == nil {
		t.Error("expected error for 0 retry")
	}

	*bad = good
	bad.Retry = math.MaxInt64
	if err := bad.Validate(); err == nil {
		t.Error("expected error for retry too large")
	}
}

func TestEffectiveCronString(t *testing.T) {
	for _, c := range []struct {
		c   string
		e   time.Duration
		exp string
	}{
		{c: "10 * * * *", exp: "10 * * * *"},
		{e: 10 * time.Second, exp: "@every 10s"},
		{exp: ""},
	} {
		o := options.Options{Cron: c.c, Every: c.e}
		got := o.EffectiveCronString()
		if got != c.exp {
			t.Fatalf("exp cron string %q, got %q for %v", c.exp, got, o)
		}
	}
}
