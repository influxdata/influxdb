package gather

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestMetrics(t *testing.T) {
	cases := []struct {
		name string
		ms   []Metrics
	}{
		{
			name: "empty",
			ms:   make([]Metrics, 0),
		},
		{
			name: "single",
			ms: []Metrics{
				{
					Timestamp: 12345,
					Tags: map[string]string{
						"b": "B",
						"a": "A",
						"c": "C",
					},
					Fields: map[string]interface{}{
						"x": 12.3,
						"y": "a long string",
					},
					Type: MetricTypeSummary,
				},
			},
		},
		{
			name: "multiple",
			ms: []Metrics{
				{
					Timestamp: 12345,
					Tags: map[string]string{
						"b": "B",
						"a": "A",
						"c": "C",
					},
					Fields: map[string]interface{}{
						"x": 12.3,
						"y": "a long string",
					},
					Type: MetricTypeSummary,
				},

				{
					Timestamp: 12345,
					Tags: map[string]string{
						"b": "B2",
						"a": "A2",
						"c": "C2",
					},
					Fields: map[string]interface{}{
						"x": 12.5,
						"y": "a long string2",
					},
					Type: MetricTypeGauge,
				},
			},
		},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.ms)
		if err != nil {
			t.Fatalf("error in marshaling metrics: %v", err)
		}
		result := make([]Metrics, 0)
		err = json.Unmarshal(b, &result)
		if err != nil {
			t.Fatalf("error in unmarshaling metrics: b: %s, %v", string(b), err)
		}
		if diff := cmp.Diff(c.ms, result, nil); diff != "" {
			t.Fatalf("unmarshaling metrics is incorrect, want %v, got %v", c.ms, result)
		}
	}
}
