package kapacitor

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/chronograf"
)

var config = `{
	"id": "93e17825-2fb0-4507-87bd-a0c136947f7e",
	"database": "telegraf",
	"measurement": "cpu",
	"retentionPolicy": "default",
	"fields": [{
		"field": "usage_user",
		"funcs": ["mean"]
	}],
	"tags": {
		"host": [
			"acc-0eabc309-eu-west-1-data-3",
			"prod"
		],
		"cpu": [
			"cpu_total"
		]
	},
	"groupBy": {
		"time": null,
		"tags": [
			"host",
			"cluster_id"
		]
	},
	"areTagsAccepted": true,
	"rawText": null
}`

func TestData(t *testing.T) {
	q := chronograf.QueryConfig{}
	err := json.Unmarshal([]byte(config), &q)
	if err != nil {
		t.Errorf("Error unmarshaling %v", err)
	}
	alert := chronograf.AlertRule{
		Trigger: "deadman",
		Query:   &q,
	}
	if tick, err := Data(alert); err != nil {
		t.Errorf("Error creating tick %v", err)
	} else {
		_, err := formatTick(tick)
		if err != nil {
			fmt.Print(tick)
			t.Errorf("Error formatting tick %v", err)
		}
	}

}
