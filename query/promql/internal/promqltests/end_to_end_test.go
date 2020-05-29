package promqltests

import (
	"testing"

	_ "github.com/influxdata/influxdb/v2/query/builtin"
)

func TestPromQLEndToEnd(t *testing.T) {
	engine, err := NewTestEngine(InputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for _, q := range TestCases {
		t.Run(q.Text, func(t *testing.T) {
			if len(q.Skip) > 0 {
				t.Skip(q.Skip)
			}
			engine.Test(t, q.Text, q.SkipComparison, q.ShouldFail, Start, End, Resolution)
		})
	}
}
