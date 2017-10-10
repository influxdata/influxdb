package kapacitor

import (
	"fmt"
	"testing"

	"github.com/influxdata/chronograf"
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
					Name: "status",
					Type: "field",
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
