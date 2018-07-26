package options_test

import (
	"fmt"
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
		body = `from(db: "test")
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
		{script: "option task = {\n  name: \"name\",\n  concurrency: 0,\n  every: 1m0s,\n\n}\n\nfrom(db: \"test\")\n    |> range(start:-1h)", shouldErr: true},
		{script: scriptGenerator(options.Options{Name: "name", Retry: 20, Every: time.Hour}, ""), shouldErr: true},
		{script: "option task = {\n  name: \"name\",\n  retry: 0,\n  every: 1m0s,\n\n}\n\nfrom(db: \"test\")\n    |> range(start:-1h)", shouldErr: true},
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
