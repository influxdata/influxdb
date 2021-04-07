package taskmodel_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/options"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

func TestUpdateValidate(t *testing.T) {
	tu := &taskmodel.TaskUpdate{}
	// this is to make sure that string durations are properly marshaled into durations
	if err := json.Unmarshal([]byte(`{"every":"3d2h", "offset":"1h"}`), tu); err != nil {
		t.Fatal(err)
	}
	if tu.Options.Every.String() != "3d2h" {
		t.Fatalf("option.every not properly unmarshaled, expected 10s got %s", tu.Options.Every)
	}
	if tu.Options.Offset.String() != "1h" {
		t.Fatalf("option.every not properly unmarshaled, expected 1h got %s", tu.Options.Offset)
	}
	if err := tu.Validate(); err != nil {
		t.Fatalf("expected task update to be valid but it was not: %s", err)
	}

}

func TestOptionsMarshal(t *testing.T) {
	tu := &taskmodel.TaskUpdate{}
	// this is to make sure that string durations are properly marshaled into durations
	if err := json.Unmarshal([]byte(`{"every":"10s", "offset":"1h"}`), tu); err != nil {
		t.Fatal(err)
	}
	if tu.Options.Every.String() != "10s" {
		t.Fatalf("option.every not properly unmarshaled, expected 10s got %s", tu.Options.Every)
	}
	if tu.Options.Offset.String() != "1h" {
		t.Fatalf("option.every not properly unmarshaled, expected 1h got %s", tu.Options.Offset)
	}

	tu = &taskmodel.TaskUpdate{}
	// this is to make sure that string durations are properly marshaled into durations
	if err := json.Unmarshal([]byte(`{"flux":"option task = {\n\tname: \"task #99\",\n\tcron: \"* * * * *\",\n\toffset: 5s,\n\tconcurrency: 100,\n}\nfrom(bucket:\"b\") |\u003e toHTTP(url:\"http://example.com\")"}`), tu); err != nil {
		t.Fatal(err)
	}

	if tu.Flux == nil {
		t.Fatalf("flux not properly unmarshaled, expected not nil but got nil")
	}
}

func TestOptionsEditWithAST(t *testing.T) {
	tu := &taskmodel.TaskUpdate{}
	tu.Options.Every = *(options.MustParseDuration("10s"))
	if err := tu.UpdateFlux(fluxlang.DefaultService, `option task = {every: 20s, name: "foo"} from(bucket:"x") |> range(start:-1h)`); err != nil {
		t.Fatal(err)
	}
	t.Run("zeroing", func(t *testing.T) {
		if !tu.Options.Every.IsZero() {
			t.Errorf("expected Every to be zeroed but it was not")
		}
	})
	t.Run("fmt string", func(t *testing.T) {
		expected := `option task = {every: 10s, name: "foo"}

from(bucket: "x")
	|> range(start: -1h)`
		if *tu.Flux != expected {
			t.Errorf("got the wrong task back, expected %s,\n got %s\n diff: %s", expected, *tu.Flux, cmp.Diff(expected, *tu.Flux))
		}
	})
	t.Run("replacement", func(t *testing.T) {
		op, err := options.FromScriptAST(fluxlang.DefaultService, *tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if op.Every.String() != "10s" {
			t.Logf("expected every to be 10s but was %s", op.Every)
			t.Fail()
		}
	})
	t.Run("add new option", func(t *testing.T) {
		tu := &taskmodel.TaskUpdate{}
		tu.Options.Offset = options.MustParseDuration("30s")
		if err := tu.UpdateFlux(fluxlang.DefaultService, `option task = {every: 20s, name: "foo"} from(bucket:"x") |> range(start:-1h)`); err != nil {
			t.Fatal(err)
		}
		op, err := options.FromScriptAST(fluxlang.DefaultService, *tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if op.Offset == nil || op.Offset.String() != "30s" {
			t.Fatalf("expected offset to be 30s but was %s", op.Offset)
		}
	})
	t.Run("switching from every to cron", func(t *testing.T) {
		tu := &taskmodel.TaskUpdate{}
		tu.Options.Cron = "* * * * *"
		if err := tu.UpdateFlux(fluxlang.DefaultService, `option task = {every: 20s, name: "foo"} from(bucket:"x") |> range(start:-1h)`); err != nil {
			t.Fatal(err)
		}
		op, err := options.FromScriptAST(fluxlang.DefaultService, *tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if !op.Every.IsZero() {
			t.Fatalf("expected every to be 0 but was %s", op.Every)
		}
		if op.Cron != "* * * * *" {
			t.Fatalf("expected Cron to be \"* * * * *\" but was %s", op.Cron)
		}
	})
	t.Run("switching from cron to every", func(t *testing.T) {
		tu := &taskmodel.TaskUpdate{}
		tu.Options.Every = *(options.MustParseDuration("10s"))
		if err := tu.UpdateFlux(fluxlang.DefaultService, `option task = {cron: "* * * * *", name: "foo"} from(bucket:"x") |> range(start:-1h)`); err != nil {
			t.Fatal(err)
		}
		op, err := options.FromScriptAST(fluxlang.DefaultService, *tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if op.Every.String() != "10s" {
			t.Fatalf("expected every to be 10s but was %s", op.Every)
		}
		if op.Cron != "" {
			t.Fatalf("expected Cron to be \"\" but was %s", op.Cron)
		}
	})
	t.Run("delete deletable option", func(t *testing.T) {
		tu := &taskmodel.TaskUpdate{}
		tu.Options.Offset = &options.Duration{}
		expscript := `option task = {cron: "* * * * *", name: "foo"}

from(bucket: "x")
	|> range(start: -1h)`
		if err := tu.UpdateFlux(fluxlang.DefaultService, `option task = {cron: "* * * * *", name: "foo", offset: 10s} from(bucket:"x") |> range(start:-1h)`); err != nil {
			t.Fatal(err)
		}
		op, err := options.FromScriptAST(fluxlang.DefaultService, *tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if !op.Every.IsZero() {
			t.Fatalf("expected every to be 0s but was %s", op.Every)
		}
		if op.Cron != "* * * * *" {
			t.Fatalf("expected Cron to be \"\" but was %s", op.Cron)
		}
		if !cmp.Equal(*tu.Flux, expscript) {
			t.Fatalf(cmp.Diff(*tu.Flux, expscript))
		}
	})

}

func TestParseRequestStillQueuedError(t *testing.T) {
	e := taskmodel.RequestStillQueuedError{Start: 1000, End: 2000}
	validMsg := e.Error()

	if err := taskmodel.ParseRequestStillQueuedError(validMsg); err == nil || *err != e {
		t.Fatalf("%q should have parsed to %v, but got %v", validMsg, e, err)
	}
}
