package gather

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsReader(t *testing.T) {
	cases := []struct {
		name  string
		ms    MetricsSlice
		wants string
		or    string
	}{
		{
			name: "single value only",
			ms: []Metrics{
				{
					Name: "cpu_load_short",
					Fields: map[string]interface{}{
						"value": 0.64,
					},
					Type:      -1,
					Timestamp: time.Unix(0, 1422568543702900257),
				},
			},
			wants: "cpu_load_short value=0.64 1422568543702900257",
		},
		{
			name: "gauge with type label produces lineprotocol with one type",
			ms: []Metrics{
				{
					Name: "error_metric",
					Tags: map[string]string{
						"type": "gauge",
					},
					Fields: map[string]interface{}{
						"value": "yes",
					},
					Type:      dto.MetricType_GAUGE,
					Timestamp: time.Unix(0, 1422568543702900257),
				},
			},
			wants: `error_metric,type=gauge value="yes" 1422568543702900257`,
		},
		{
			name: "single regular metrics",
			ms: []Metrics{
				{
					Name: "cpu_load_short",
					Tags: map[string]string{
						"host":   "server01",
						"region": "us-west",
					},
					Fields: map[string]interface{}{
						"value": 0.64,
					},
					Type:      dto.MetricType_GAUGE,
					Timestamp: time.Unix(0, 1422568543702900257),
				},
			},
			wants: "cpu_load_short,host=server01,region=us-west value=0.64 1422568543702900257",
		},
		{
			name: "multiple value only",
			ms: []Metrics{
				{
					Name: "cpu_load_short",
					Fields: map[string]interface{}{
						"value":  0.64,
						"region": "us-west",
					},
					Type:      -1,
					Timestamp: time.Unix(0, 1522568543702900257),
				},
			},
			wants: `cpu_load_short region="us-west",value=0.64 1522568543702900257`,
		},
		{
			name: "multiple metrics",
			ms: []Metrics{
				{
					Name: "cpu_load_short",
					Tags: map[string]string{
						"region": "us-west",
					},
					Fields: map[string]interface{}{
						"value": 0.64,
					},
					Type:      -1,
					Timestamp: time.Unix(0, 1422568543702900257),
				},
				{
					Name: "cpu_load_short",
					Tags: map[string]string{
						"region": "us-east",
					},
					Fields: map[string]interface{}{
						"value": 0.34,
					},
					Type:      -1,
					Timestamp: time.Unix(0, 1522568543702900257),
				},
			},
			wants: "cpu_load_short,region=us-west value=0.64 1422568543702900257\ncpu_load_short,region=us-east value=0.34 1522568543702900257",
		},
	}
	for _, c := range cases {
		r, err := c.ms.Reader()
		if err != nil {
			t.Fatalf("error in convert metrics to reader: %v", err)
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(r)

		if diff1 := cmp.Diff(c.wants, buf.String(), nil); diff1 != "" {
			if diff2 := cmp.Diff(c.or, buf.String(), nil); diff2 != "" {
				t.Fatalf("convert metrics is incorrect, diff %s", diff1)
			}
		}
	}
}

func TestMetricsMarshal(t *testing.T) {
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
					Timestamp: time.Unix(12345, 0),
					Tags: map[string]string{
						"b": "B",
						"a": "A",
						"c": "C",
					},
					Fields: map[string]interface{}{
						"x": 12.3,
						"y": "a long string",
					},
					Type: dto.MetricType_SUMMARY,
				},
			},
		},
		{
			name: "multiple",
			ms: []Metrics{
				{
					Timestamp: time.Unix(12345, 0),
					Tags: map[string]string{
						"b": "B",
						"a": "A",
						"c": "C",
					},
					Fields: map[string]interface{}{
						"x": 12.3,
						"y": "a long string",
					},
					Type: dto.MetricType_SUMMARY,
				},

				{
					Timestamp: time.Unix(12345, 0),
					Tags: map[string]string{
						"b": "B2",
						"a": "A2",
						"c": "C2",
					},
					Fields: map[string]interface{}{
						"x": 12.5,
						"y": "a long string2",
					},
					Type: dto.MetricType_GAUGE,
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
			t.Fatalf("error in unmarshalling metrics: b: %s, %v", string(b), err)
		}
		if diff := cmp.Diff(c.ms, result, nil); diff != "" {
			t.Fatalf("unmarshalling metrics is incorrect, want %v, got %v", c.ms, result)
		}
	}
}
