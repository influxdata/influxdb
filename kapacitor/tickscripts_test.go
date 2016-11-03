package kapacitor

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestGenerate(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:          "name",
		Version:       "1.0",
		Trigger:       "relative",
		AlertServices: []string{"slack", "victorOps", "email"},
		Type:          "stream",
		Operator:      ">",
		Aggregate:     "mean",
		Period:        "10m",
		Every:         "30s",
		Critical:      "90",
		Shift:         "1m",
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
	_, err := gen.Generate(alert)
	if err != nil {
		t.Errorf("Error generating alert: %v", err)
	}
}

func TestThreshold(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:          "name",
		Version:       "1.0",
		Trigger:       "threshold",
		AlertServices: []string{"slack", "victorOps", "email"},
		Type:          "stream",
		Operator:      ">",
		Aggregate:     "mean",
		Period:        "10m",
		Every:         "30s",
		Critical:      "90",
		Shift:         "1m",
		Message:       "message",
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

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var levelfield = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var metric = 'metric'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

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
        .as(metric)

var trigger = data
    |mean(metric)
        .as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
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
        .tag('name', 'name')
        .tag('type', 'stream')
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
			t.Errorf("%q. Threshold() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestRelative(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:          "name",
		Version:       "1.0",
		Trigger:       "relative",
		AlertServices: []string{"slack", "victorOps", "email"},
		Type:          "stream",
		Operator:      ">",
		Aggregate:     "mean",
		Period:        "10m",
		Every:         "30s",
		Critical:      "90",
		Shift:         "1m",
		Message:       "message",
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

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var levelfield = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var metric = 'metric'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

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
        .as(metric)

var past = data
    |mean(metric)
        .as('stat')
    |shift(shift)

var current = data
    |mean(metric)
        .as('stat')

var trigger = past
    |join(current)
        .as('past', 'current')
    |eval(lambda: abs(float("current.stat" - "past.stat")) / float("past.stat"))
        .keep()
        .as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" > crit)
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
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
        .tag('name', 'name')
        .tag('type', 'stream')
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
			t.Errorf("%q. Relative() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestDeadman(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:          "name",
		Version:       "1.0",
		Trigger:       "deadman",
		AlertServices: []string{"slack", "victorOps", "email"},
		Type:          "stream",
		Operator:      ">",
		Aggregate:     "mean",
		Period:        "10m",
		Every:         "30s",
		Critical:      "90",
		Shift:         "1m",
		Message:       "message",
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

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idtag = 'alertID'

var levelfield = 'level'

var messagefield = 'message'

var durationfield = 'duration'

var metric = 'metric'

var output_db = 'chronograf'

var output_rp = 'autogen'

var output_mt = 'alerts'

var threshold = 0.0

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
        .as(metric)

var trigger = data
    |deadman(threshold, every)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idtag)
        .levelField(levelfield)
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
        .tag('name', 'name')
        .tag('type', 'stream')
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
			t.Errorf("%q. Deadman() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
