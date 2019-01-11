package influxdb_test

import (
	"encoding/json"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/options"
)

func TestOptionsMarshal(t *testing.T) {
	tu := &platform.TaskUpdate{}
	// this is to make sure that string durations are properly marshaled into durations
	if err := json.Unmarshal([]byte(`{"every":"10s", "offset":"1h"}`), tu); err != nil {
		t.Fatal(err)
	}
	if tu.Options.Every != 10*time.Second {
		t.Fatalf("option.every not properly unmarshaled, expected 10s got %s", tu.Options.Every)
	}
	if tu.Options.Offset != time.Hour {
		t.Fatalf("option.every not properly unmarshaled, expected 1h got %s", tu.Options.Offset)
	}

	tu = &platform.TaskUpdate{}
	// this is to make sure that string durations are properly marshaled into durations
	if err := json.Unmarshal([]byte(`{"flux":"option task = {\n\tname: \"task #99\",\n\tcron: \"* * * * *\",\n\toffset: 5s,\n\tconcurrency: 100,\n}\nfrom(bucket:\"b\") |\u003e toHTTP(url:\"http://example.com\")"}`), tu); err != nil {
		t.Fatal(err)
	}

	if tu.Flux == nil {
		t.Fatalf("flux not properly unmarshaled, expected not nil but got nil")
	}
}

func TestOptionsEdit(t *testing.T) {
	tu := &platform.TaskUpdate{}
	tu.Options.Every = 10 * time.Second
	if err := tu.UpdateFlux(`option task = {every: 20s, name: "foo"} from(bucket:"x") |> range(start:-1h)`); err != nil {
		t.Fatal(err)
	}
	t.Run("test zeroing", func(t *testing.T) {
		if tu.Options.Every != 0 {
			t.Errorf("expected Every to be zeroed but it wasn't")
		}
	})
	t.Run("test fmt string", func(t *testing.T) {
		t.Skip("This won't work until the flux formatter formats durations in a nicer way")
		expected := `option task = {every: 10s, name: "foo"}
from(bucket:"x")
|> range(start:-1h)`
		if *tu.Flux != expected {
			t.Errorf("got the wrong task back, expected %s,\n got %s\n", expected, *tu.Flux)
		}
	})
	t.Run("test replacement", func(t *testing.T) {
		op, err := options.FromScript(*tu.Flux)
		if err != nil {
			t.Error(err)
		}
		if op.Every != 10*time.Second {
			t.Logf("expected every to be 10s but was %s", op.Every)
			t.Fail()
		}
	})
}
