package kapacitor

import (
	"fmt"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestGenerate(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "relative",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Change:   "change",
			Shift:    "1m",
			Operator: "greater than",
			Value:    "90",
		},
		Every: "30s",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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
		t.Errorf("Error generating alert: %v %s", err, tick)
	}
}

func TestThreshold(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Operator: "greater than",
			Value:    "90",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestThresholdDetail(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Operator: "greater than",
			Value:    "90",
		},
		Every:   "30s",
		Message: "message",
		Details: "details",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var details = 'details'

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .details(details)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestThresholdInsideRange(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Operator:   "inside range",
			Value:      "90",
			RangeValue: "100",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var lower = 90

var upper = 100

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" >= lower AND "value" <= upper)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestThresholdOutsideRange(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Operator:   "outside range",
			Value:      "90",
			RangeValue: "100",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var lower = 90

var upper = 100

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" < lower OR "value" > upper)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestThresholdNoAggregate(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "threshold",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Operator: "greater than",
			Value:    "90",
		},
		Every:   "30s",
		Message: "message",
		Query: chronograf.QueryConfig{
			Database:        "telegraf",
			Measurement:     "cpu",
			RetentionPolicy: "autogen",
			Fields: []chronograf.Field{
				{
					Field: "usage_user",
					Funcs: []string{},
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "usage_user")
        .as('value')

var trigger = data
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestRelative(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "relative",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Change:   "% change",
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
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'relative'

var shift = 1m

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var past = data
    |shift(shift)

var current = data

var trigger = past
    |join(current)
        .as('past', 'current')
    |eval(lambda: abs(float("current.value" - "past.value")) / float("past.value") * 100.0)
        .keep()
        .as('value')
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}

func TestRelativeChange(t *testing.T) {
	alert := chronograf.AlertRule{
		Name:    "name",
		Trigger: "relative",
		Alerts:  []string{"slack", "victorops", "email"},
		TriggerValues: chronograf.TriggerValues{
			Change:   "change",
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
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
				Time: "10m",
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var every = 30s

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'relative'

var shift = 1m

var crit = 90

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |window()
        .period(period)
        .every(every)
        .align()
    |mean('usage_user')
        .as('value')

var past = data
    |shift(shift)

var current = data

var trigger = past
    |join(current)
        .as('past', 'current')
    |eval(lambda: float("current.value" - "past.value"))
        .keep()
        .as('value')
    |alert()
        .crit(lambda: "value" > crit)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
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
			Fields: []chronograf.Field{
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
			GroupBy: chronograf.GroupBy{
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

var groupBy = ['host', 'cluster_id']

var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

var period = 10m

var name = 'name'

var idVar = name + ':{{.Group}}'

var message = 'message'

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'deadman'

var threshold = 0.0

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)

var trigger = data
    |deadman(threshold, period)
        .stateChangesOnly()
        .message(message)
        .id(idVar)
        .idTag(idTag)
        .levelTag(levelTag)
        .messageField(messageField)
        .durationField(durationField)
        .slack()
        .victorOps()
        .email()

trigger
    |eval(lambda: "emitted")
        .as('value')
        .keep('value', messageField, durationField)
    |influxDBOut()
        .create()
        .database(outputDB)
        .retentionPolicy(outputRP)
        .measurement(outputMeasurement)
        .tag('alertName', name)
        .tag('triggerType', triggerType)

trigger
    |httpOut('output')
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
			diff := diffmatchpatch.New()
			delta := diff.DiffMain(string(tt.want), string(got), true)
			t.Errorf("%q\n%s", tt.name, diff.DiffPrettyText(delta))
		}
	}
}
