package chronograf_test

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func Test_GroupByVar(t *testing.T) {
	gbvTests := []struct {
		name       string
		query      string
		want       string
		resolution uint // the screen resolution to render queries into
	}{
		{
			name:       "relative time only lower bound with one day of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d GROUP BY :interval:",
			resolution: 1000,
			want:       "time(259s)",
		},
		{
			name:       "relative time with relative upper bound with one minute of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 3m  AND time < now() - 2m GROUP BY :interval:",
			resolution: 1000,
			want:       "time(180ms)",
		},
		{
			name:       "relative time with relative lower bound and now upper with one day of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > now() - 1d  AND time < now() GROUP BY :interval:",
			resolution: 1000,
			want:       "time(259s)",
		},
		{
			name:       "absolute time with one minute of duration",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > '1985-10-25T00:01:00Z' and time < '1985-10-25T00:02:00Z' GROUP BY :interval:",
			resolution: 1000,
			want:       "time(180ms)",
		},
		{
			name:       "absolute time with nano seconds and zero duraiton",
			query:      "SELECT mean(usage_idle) FROM cpu WHERE time > '2017-07-24T15:33:42.994Z' and time < '2017-07-24T15:33:42.994Z' GROUP BY :interval:",
			resolution: 1000,
			want:       "time(1ms)",
		},
	}

	for _, test := range gbvTests {
		t.Run(test.name, func(t *testing.T) {
			gbv := chronograf.GroupByVar{
				Var:        ":interval:",
				Resolution: test.resolution,
			}

			gbv.Exec(test.query)
			got := gbv.String()

			if got != test.want {
				t.Fatalf("%q - durations not equal! Want: %s, Got: %s", test.name, test.want, got)
			}
		})
	}
}
