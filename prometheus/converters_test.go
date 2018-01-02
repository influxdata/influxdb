package prometheus_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/influxdata/influxql"
)

func TestReadRequestToInfluxQLQuery(t *testing.T) {
	examples := []struct {
		name     string
		queries  []*remote.Query
		expQuery string
		expError error
	}{
		{
			name:     "too many queries",
			queries:  []*remote.Query{{}, {}}, // Multiple queries
			expError: errors.New("Prometheus read endpoint currently only supports one query at a time"),
		},
		{
			name: "single condition",
			queries: []*remote.Query{{
				StartTimestampMs: 1,
				EndTimestampMs:   100,
				Matchers: []*remote.LabelMatcher{
					{Name: "region", Value: "west", Type: remote.MatchType_EQUAL},
				},
			}},
			expQuery: "SELECT f64 FROM db0.rp0._ WHERE region = 'west' AND time >= '1970-01-01T00:00:00.001Z' AND time <= '1970-01-01T00:00:00.1Z' GROUP BY *",
		},
		{
			name: "multiple conditions",
			queries: []*remote.Query{{
				StartTimestampMs: 1,
				EndTimestampMs:   100,
				Matchers: []*remote.LabelMatcher{
					{Name: "region", Value: "west", Type: remote.MatchType_EQUAL},
					{Name: "host", Value: "serverA", Type: remote.MatchType_NOT_EQUAL},
				},
			}},
			expQuery: "SELECT f64 FROM db0.rp0._ WHERE region = 'west' AND host != 'serverA' AND time >= '1970-01-01T00:00:00.001Z' AND time <= '1970-01-01T00:00:00.1Z' GROUP BY *",
		},
		{
			name: "rewrite regex",
			queries: []*remote.Query{{
				StartTimestampMs: 1,
				EndTimestampMs:   100,
				Matchers: []*remote.LabelMatcher{
					{Name: "region", Value: "c.*", Type: remote.MatchType_REGEX_MATCH},
					{Name: "host", Value: `\d`, Type: remote.MatchType_REGEX_NO_MATCH},
				},
			}},
			expQuery: `SELECT f64 FROM db0.rp0._ WHERE region =~ /c.*/ AND host !~ /\d/ AND time >= '1970-01-01T00:00:00.001Z' AND time <= '1970-01-01T00:00:00.1Z' GROUP BY *`,
		},
		{
			name: "escape regex",
			queries: []*remote.Query{{
				StartTimestampMs: 1,
				EndTimestampMs:   100,
				Matchers: []*remote.LabelMatcher{
					{Name: "test_type", Value: "a/b", Type: remote.MatchType_REGEX_MATCH},
				},
			}},
			expQuery: `SELECT f64 FROM db0.rp0._ WHERE test_type =~ /a\/b/ AND time >= '1970-01-01T00:00:00.001Z' AND time <= '1970-01-01T00:00:00.1Z' GROUP BY *`,
		},
	}

	for _, example := range examples {
		t.Run(example.name, func(t *testing.T) {
			readRequest := &remote.ReadRequest{Queries: example.queries}
			query, err := prometheus.ReadRequestToInfluxQLQuery(readRequest, "db0", "rp0")
			if !reflect.DeepEqual(err, example.expError) {
				t.Errorf("got error %v, expected %v", err, example.expError)
			}

			var queryString string
			if query != nil {
				queryString = query.String()
			}

			if queryString != example.expQuery {
				t.Errorf("got query %v, expected %v", queryString, example.expQuery)
			}

			if queryString != "" {
				if _, err := influxql.ParseStatement(queryString); err != nil {
					t.Error(err)
				}
			}
		})
	}
}
