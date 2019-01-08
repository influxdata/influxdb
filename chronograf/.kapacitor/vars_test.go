package kapacitor

import (
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
)

func TestVarsCritStringEqual(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		TriggerValues: chronograf.TriggerValues{
			Operator: "equal to",
			Value:    "DOWN",
		},
		Every: "30s",
		Query: &chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "haproxy",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
				{
					Value: "status",
					Type:  "field",
				},
			},
			GroupBy: chronograf.GroupBy{
				Time: "10m",
				Tags: []string{"pxname"},
			},
			AreTagsAccepted: true,
		},
	}

	raw, err := Vars(alert)
	if err != nil {
		fmt.Printf("%s", raw)
		t.Fatalf("Error generating alert: %v %s", err, raw)
	}

	tick, err := formatTick(raw)
	if err != nil {
		t.Errorf("Error formatting alert: %v %s", err, raw)
	}

	if err := validateTick(tick); err != nil {
		t.Errorf("Error validating alert: %v %s", err, tick)
	}
}

func Test_formatValue(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			name:  "parses floats",
			value: "3.14",
			want:  "3.14",
		},
		{
			name:  "parses booleans",
			value: "TRUE",
			want:  "TRUE",
		},
		{
			name:  "single quotes for strings",
			value: "up",
			want:  "'up'",
		},
		{
			name:  "handles escaping of single quotes",
			value: "down's",
			want:  "'down\\'s'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatValue(tt.value); got != tt.want {
				t.Errorf("formatValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
