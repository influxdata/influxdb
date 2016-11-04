package kapacitor

import (
	"fmt"
	"testing"

	"github.com/influxdata/chronograf"
)

func TestGenerate(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "relative",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Change:   "change",
			Period:   "10m",
			Shift:    "1m",
			Operator: "greater than",
			Value:    "90",
		},
		Every: "30s",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []struct {
				Field string   `json:"field"`
				Funcs []string `json:"funcs"`
			}{
				{
					Field: "usage_user",
					Funcs: []string{"mean"},
				},
			},
			Tags: map[string][]string{
				"host": []string{
					"acc-0eabc309-eu-west-1-data-3",
					"prod",
				},
				"cpu": []string{
					"cpu_total",
				},
			},
			GroupBy: struct {
				Time string   `json:"time"`
				Tags []string `json:"tags"`
			}{
				Time: "",
				Tags: []string{"host", "cluster_id"},
			},
			AreTagsAccepted: true,
			RawText:         "",
		},
	}
	gen := Alert{}
	tick, err := gen.Generate(alert)
	if err != nil {
		fmt.Printf("%s", tick)
		t.Errorf("Error generating alert: %v", err)
	}
}

func TestThreshold(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Relation:   "once",
			Period:     "10m",
			Percentile: "", // TODO: if relation is not once then this will have a number
			Operator:   "greater than",
			Value:      "90",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []struct {
				Field string   `json:"field"`
				Funcs []string `json:"funcs"`
			}{
				{
					Field: "usage_user",
					Funcs: []string{"mean"},
				},
			},
			Tags: map[string][]string{
				"host": []string{
					"acc-0eabc309-eu-west-1-data-3",
					"prod",
				},
				"cpu": []string{
					"cpu_total",
				},
			},
			GroupBy: struct {
				Time string   `json:"time"`
				Tags []string `json:"tags"`
			}{
				Time: "",
				Tags: []string{"host", "cluster_id"},
			},
			AreTagsAccepted: true,
			RawText:         "",
		},
	}

	tests := []struct {
		name    string
		alert   chronograf.AlertRule
		want    chronograf.TICKScript
		wantErr bool
	}{
		{
			name:  "Test valid template alert",
			alert: alert,
			want: `var db = 'telegraf'

var rp = 'autogen'

var measurement = 'cpu'

var field = 'usage_user'

var groupby = ['host', 'cluster_id']

var where_filter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var leveltag = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

var triggerType = 'threshold'

var every = 30s

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)
        .where(where_filter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean(field)
        .as('value')

var trigger = data
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelTag(leveltag)
        .messageField(messagefield)
        .durationField(durationfield)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(output_db)
        .retentionPolicy(output_rp)
        .measurement(output_mt)
        .tag('alertName', name)
        .tag('triggerType', triggerType)
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		gen := Alert{}
		got, err := gen.Generate(tt.alert)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Threshold() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			fmt.Printf("%s", got)
			t.Errorf("%q. Threshold() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestRelative(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "relative",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Change:   "change",
			Period:   "10m",
			Shift:    "1m",
			Operator: "greater than",
			Value:    "90",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []struct {
				Field string   `json:"field"`
				Funcs []string `json:"funcs"`
			}{
				{
					Field: "usage_user",
					Funcs: []string{"mean"},
				},
			},
			Tags: map[string][]string{
				"host": []string{
					"acc-0eabc309-eu-west-1-data-3",
					"prod",
				},
				"cpu": []string{
					"cpu_total",
				},
			},
			GroupBy: struct {
				Time string   `json:"time"`
				Tags []string `json:"tags"`
			}{
				Time: "",
				Tags: []string{"host", "cluster_id"},
			},
			AreTagsAccepted: true,
			RawText:         "",
		},
	}

	tests := []struct {
		name    string
		alert   chronograf.AlertRule
		want    chronograf.TICKScript
		wantErr bool
	}{
		{
			name:  "Test valid template alert",
			alert: alert,
			want: `var db = 'telegraf'

var rp = 'autogen'

var measurement = 'cpu'

var field = 'usage_user'

var groupby = ['host', 'cluster_id']

var where_filter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var leveltag = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

var triggerType = 'relative'

var every = 30s

var shift = -1m

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)
        .where(where_filter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean(field)
        .as('value')

var past = data
    |shift(shift)

var current = data

var trigger = past
    |join(current)
        .as('past', 'current')
    |eval(lambda: abs(float("current.value" - "past.value")) / float("past.value"))
        .keep()
        .as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelTag(leveltag)
        .messageField(messagefield)
        .durationField(durationfield)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(output_db)
        .retentionPolicy(output_rp)
        .measurement(output_mt)
        .tag('alertName', name)
        .tag('triggerType', triggerType)
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		gen := Alert{}
		got, err := gen.Generate(tt.alert)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Relative() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			fmt.Printf("%s", got)
			t.Errorf("%q. Relative() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDeadman(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "deadman",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Period: "10m",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []struct {
				Field string   `json:"field"`
				Funcs []string `json:"funcs"`
			}{
				{
					Field: "usage_user",
					Funcs: []string{"mean"},
				},
			},
			Tags: map[string][]string{
				"host": []string{
					"acc-0eabc309-eu-west-1-data-3",
					"prod",
				},
				"cpu": []string{
					"cpu_total",
				},
			},
			GroupBy: struct {
				Time string   `json:"time"`
				Tags []string `json:"tags"`
			}{
				Time: "",
				Tags: []string{"host", "cluster_id"},
			},
			AreTagsAccepted: true,
			RawText:         "",
		},
	}

	tests := []struct {
		name    string
		alert   chronograf.AlertRule
		want    chronograf.TICKScript
		wantErr bool
	}{
		{
			name:  "Test valid template alert",
			alert: alert,
			want: `var db = 'telegraf'

var rp = 'autogen'

var measurement = 'cpu'

var field = 'usage_user'

var groupby = ['host', 'cluster_id']

var where_filter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var leveltag = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

var triggerType = 'deadman'

var threshold = 0.0

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupby)
        .where(where_filter)

var trigger = data
    |deadman(threshold, period)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelTag(leveltag)
        .messageField(messagefield)
        .durationField(durationfield)
        .slack()
        .victorOps()
        .email()

trigger
    |eval(lambda: field)
        .as('value')
    |influxDBOut()
        .create()
        .database(output_db)
        .retentionPolicy(output_rp)
        .measurement(output_mt)
        .tag('alertName', name)
        .tag('triggerType', triggerType)
`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		gen := Alert{}
		got, err := gen.Generate(tt.alert)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Deadman() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			fmt.Printf("%s", got)
			t.Errorf("%q. Deadman() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
