package options_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/ast"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/options"
)

func scriptGenerator(opt options.Options, body string) string {
	taskData := ""
	if opt.Name != "" {
		taskData = fmt.Sprintf("%s  name: %q,\n", taskData, opt.Name)
	}
	if opt.Cron != "" {
		taskData = fmt.Sprintf("%s  cron: %q,\n", taskData, opt.Cron)
	}
	if !opt.Every.IsZero() {
		taskData = fmt.Sprintf("%s  every: %s,\n", taskData, opt.Every.String())
	}
	if opt.Offset != nil && !(*opt.Offset).IsZero() {
		taskData = fmt.Sprintf("%s  offset: %s,\n", taskData, opt.Offset.String())
	}
	if opt.Concurrency != nil && *opt.Concurrency != 0 {
		taskData = fmt.Sprintf("%s  concurrency: %d,\n", taskData, *opt.Concurrency)
	}
	if opt.Retry != nil && *opt.Retry != 0 {
		taskData = fmt.Sprintf("%s  retry: %d,\n", taskData, *opt.Retry)
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

func TestNegDurations(t *testing.T) {
	dur := options.MustParseDuration("-1m")
	d, err := dur.DurationFrom(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if d != -time.Minute {
		t.Fatalf("expected duration to be -1m but was %s", d)
	}
}

func TestFromScriptAST(t *testing.T) {
	for _, c := range []struct {
		script    string
		exp       options.Options
		shouldErr bool
	}{
		{script: scriptGenerator(options.Options{Name: "name0", Cron: "* * * * *", Concurrency: pointer.Int64(2), Retry: pointer.Int64(3), Offset: options.MustParseDuration("-1m")}, ""),
			exp: options.Options{Name: "name0",
				Cron:        "* * * * *",
				Concurrency: pointer.Int64(2),
				Retry:       pointer.Int64(3),
				Offset:      options.MustParseDuration("-1m")}},
		{script: scriptGenerator(options.Options{Name: "name1", Every: *(options.MustParseDuration("5s"))}, ""), exp: options.Options{Name: "name1", Every: *(options.MustParseDuration("5s")), Concurrency: pointer.Int64(1), Retry: pointer.Int64(1)}},
		{script: scriptGenerator(options.Options{Name: "name2", Cron: "* * * * *"}, ""), exp: options.Options{Name: "name2", Cron: "* * * * *", Concurrency: pointer.Int64(1), Retry: pointer.Int64(1)}},
		{script: scriptGenerator(options.Options{Name: "name3", Every: *(options.MustParseDuration("1h")), Cron: "* * * * *"}, ""), shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name4", Concurrency: pointer.Int64(1000), Every: *(options.MustParseDuration("1h"))}, ""), shouldErr: true},
		{script: "option task = {\n  name: \"name5\",\n  concurrency: 0,\n  every: 1m0s,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: "option task = {\n  name: \"name6\",\n  concurrency: 1,\n  every: 1,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name7", Retry: pointer.Int64(20), Every: *(options.MustParseDuration("1h"))}, ""), shouldErr: true},
		{script: "option task = {\n  name: \"name8\",\n  retry: 0,\n  every: 1m0s,\n\n}\n\nfrom(bucket: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name9"}, ""), shouldErr: true},
		{script: scriptGenerator(options.Options{}, ""), shouldErr: true},
		{script: `option task = {
			name: "name10",
			every: 1d,
			offset: 1m,
		}
			from(bucket: "metrics")
			|> range(start: now(), stop: 8w)
		`,
			exp: options.Options{Name: "name10", Every: *(options.MustParseDuration("1d")), Concurrency: pointer.Int64(1), Retry: pointer.Int64(1), Offset: options.MustParseDuration("1m")},
		},
		{script: `option task = {
			name: "name11",
			every: 1m,
			offset: 1d,
		}
			from(bucket: "metrics")
			|> range(start: now(), stop: 8w)

		`,
			exp: options.Options{Name: "name11", Every: *(options.MustParseDuration("1m")), Concurrency: pointer.Int64(1), Retry: pointer.Int64(1), Offset: options.MustParseDuration("1d")},
		},
		{script: "option task = {name:\"test_task_smoke_name\", every:30s} from(bucket:\"test_tasks_smoke_bucket_source\") |> range(start: -1h) |> map(fn: (r) => ({r with _time: r._time, _value:r._value, t : \"quality_rocks\"}))|> to(bucket:\"test_tasks_smoke_bucket_dest\", orgID:\"3e73e749495d37d5\")",
			exp: options.Options{Name: "test_task_smoke_name", Every: *(options.MustParseDuration("30s")), Retry: pointer.Int64(1), Concurrency: pointer.Int64(1)}, shouldErr: false}, // TODO(docmerlin): remove this once tasks fully supports all flux duration units.

	} {
		o, err := options.FromScriptAST(fluxlang.DefaultService, c.script)
		if c.shouldErr && err == nil {
			t.Fatalf("script %q should have errored but didn't", c.script)
		} else if !c.shouldErr && err != nil {
			t.Fatalf("script %q should not have errored, but got %v", c.script, err)
		}

		if err != nil {
			continue
		}

		ignoreLocation := cmpopts.IgnoreFields(ast.BaseNode{}, "Loc")

		if !cmp.Equal(o, c.exp, ignoreLocation) {
			t.Fatalf("script %q got unexpected result -got/+exp\n%s", c.script, cmp.Diff(o, c.exp))
		}
	}
}

func TestValidate(t *testing.T) {
	good := options.Options{Name: "x", Cron: "* * * * *", Concurrency: pointer.Int64(1), Retry: pointer.Int64(1)}
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
	bad.Every = *options.MustParseDuration("1m")
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
	bad.Every = *options.MustParseDuration("-1m")
	if err := bad.Validate(); err == nil {
		t.Error("expected error for negative every")
	}

	*bad = good
	bad.Offset = options.MustParseDuration("1500ms")
	if err := bad.Validate(); err == nil {
		t.Error("expected error for sub-second delay resolution")
	}

	*bad = good
	bad.Concurrency = pointer.Int64(0)
	if err := bad.Validate(); err == nil {
		t.Error("expected error for 0 concurrency")
	}

	*bad = good
	bad.Concurrency = pointer.Int64(math.MaxInt64)
	if err := bad.Validate(); err == nil {
		t.Error("expected error for concurrency too large")
	}

	*bad = good
	bad.Retry = pointer.Int64(0)
	if err := bad.Validate(); err == nil {
		t.Error("expected error for 0 retry")
	}

	*bad = good
	bad.Retry = pointer.Int64(math.MaxInt64)
	if err := bad.Validate(); err == nil {
		t.Error("expected error for retry too large")
	}

	notbad := new(options.Options)
	*notbad = good
	notbad.Cron = ""
	notbad.Every = *options.MustParseDuration("22d")
	if err := notbad.Validate(); err != nil {
		t.Error("expected no error for days every")
	}

}

func TestEffectiveCronString(t *testing.T) {
	for _, c := range []struct {
		c   string
		e   options.Duration
		exp string
	}{
		{c: "10 * * * *", exp: "10 * * * *"},
		{e: *(options.MustParseDuration("10s")), exp: "@every 10s"},
		{exp: ""},
		{e: *(options.MustParseDuration("10d")), exp: "@every 10d"},
	} {
		o := options.Options{Cron: c.c, Every: c.e}
		got := o.EffectiveCronString()
		if got != c.exp {
			t.Fatalf("exp cron string %q, got %q for %v", c.exp, got, o)
		}
	}
}

func TestDurationMarshaling(t *testing.T) {
	t.Run("unmarshalling", func(t *testing.T) {
		now := time.Now().UTC() /* to guarantee 24 hour days*/
		dur1 := options.Duration{}
		if err := dur1.UnmarshalText([]byte("1d1h10m3s")); err != nil {
			t.Fatal(err)
		}
		d1, err1 := dur1.DurationFrom(now)
		if err1 != nil {
			t.Fatal(err1)
		}

		dur2 := options.Duration{}
		if err := dur2.Parse("1d1h10m3s"); err != nil {
			t.Fatal(err)
		}
		d2, err2 := dur2.DurationFrom(now)
		if err2 != nil {
			t.Fatal(err2)
		}

		if d1 != d2 || d1 != 25*time.Hour+10*time.Minute+3*time.Second /* we know that this day is 24 hours long because its UTC and go ignores leap seconds*/ {
			t.Fatal("Parse and Marshaling do not give us the same result")
		}
	})

	t.Run("marshaling", func(t *testing.T) {
		dur := options.Duration{}
		if err := dur.UnmarshalText([]byte("1h10m3s")); err != nil {
			t.Fatal(err)
		}
		if dur.String() != "1h10m3s" {
			t.Fatalf("duration string should be \"1h10m3s\" but was %s", dur.String())
		}
		text, err := dur.MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		if string(text) != "1h10m3s" {
			t.Fatalf("duration text should be \"1h10m3s\" but was %s", text)
		}
	})

	t.Run("parse zero", func(t *testing.T) {
		dur := options.Duration{}
		if err := dur.UnmarshalText([]byte("0h0s")); err != nil {
			t.Fatal(err)
		}
		if !dur.IsZero() {
			t.Fatalf("expected duration \"0s\" to be zero but was %s", dur.String())
		}
	})
}

func TestDurationMath(t *testing.T) {
	dur := options.MustParseDuration("10s")
	d, err := dur.DurationFrom(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if d != 10*time.Second {
		t.Fatalf("expected duration to be 10s but it was %s", d)
	}
}
