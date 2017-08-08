package chronograf_test

import (
	"testing"
	"time"

	"github.com/influxdata/chronograf"
)

func Test_GroupByVar(t *testing.T) {
	gbvTests := []struct {
		name              string
		query             string
		expected          time.Duration
		resolution        uint // the screen resolution to render queries into
		reportingInterval time.Duration
	}{
		{
			"relative time",
			"SELECT mean(usage_idle) FROM cpu WHERE time > now() - 180d GROUP BY :interval:",
			4320 * time.Hour,
			1000,
			10 * time.Second,
		},
		{
			"absolute time",
			"SELECT mean(usage_idle) FROM cpu WHERE time > '1985-10-25T00:01:00Z' and time < '1985-10-25T00:02:00Z' GROUP BY :interval:",
			1 * time.Minute,
			1000,
			10 * time.Second,
		},
	}

	for _, test := range gbvTests {
		t.Run(test.name, func(t *testing.T) {
			gbv := chronograf.GroupByVar{
				Var:               ":interval:",
				Resolution:        test.resolution,
				ReportingInterval: test.reportingInterval,
			}

			gbv.Exec(test.query)

			if gbv.Duration != test.expected {
				t.Fatalf("%q - durations not equal! Want: %s, Got: %s", test.name, test.expected, gbv.Duration)
			}
		})
	}
}
