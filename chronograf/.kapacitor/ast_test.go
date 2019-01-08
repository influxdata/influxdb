package kapacitor

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/chronograf"
)

func TestReverse(t *testing.T) {
	tests := []struct {
		name    string
		script  chronograf.TICKScript
		want    chronograf.AlertRule
		wantErr bool
	}{
		{
			name: "simple stream tickscript",
			script: chronograf.TICKScript(`
														var name = 'name'
														var triggerType = 'threshold'
														var every = 30s
														var period = 10m
														var groupBy = ['host', 'cluster_id']
														var db = 'telegraf'
														var rp = 'autogen'
														var measurement = 'cpu'
														var message = 'message'
														var details = 'details'
														var crit = 90
														var idVar = name + ':{{.Group}}'
														var idTag = 'alertID'
														var levelTag = 'level'
														var messageField = 'message'
														var durationField = 'duration'
														var whereFilter = lambda: ("cpu" == 'cpu_total') AND ("host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')

														var data = stream
														|from()
															.database(db)
															.retentionPolicy(rp)
															.measurement(measurement)
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
															.email('howdy@howdy.com', 'doody@doody.com')
															.log('/tmp/alerts.log')
															.post('http://backin.tm')
															.endpoint('myendpoint')
															.header('key', 'value')
															`),

			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{
							To: []string{"howdy@howdy.com", "doody@doody.com"},
						},
					},
					Log: []*chronograf.Log{
						{
							FilePath: "/tmp/alerts.log",
						},
					},
					Posts: []*chronograf.Post{
						{
							URL:     "http://backin.tm",
							Headers: map[string]string{"key": "value"},
						},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90",
				},
				Every:   "30s",
				Message: "message",
				Details: "details",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					RetentionPolicy: "autogen",
					Measurement:     "cpu",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
						},
					},
					GroupBy: chronograf.GroupBy{
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					Tags: map[string][]string{
						"cpu": []string{
							"cpu_total",
						},
						"host": []string{
							"acc-0eabc309-eu-west-1-data-3",
							"prod",
						},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test Threshold",
			script: `var db = 'telegraf'

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
		    |httpOut('output')`,
			want: chronograf.AlertRule{
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
						},
					},
					Tags: map[string][]string{
						"cpu":  []string{"cpu_total"},
						"host": []string{"acc-0eabc309-eu-west-1-data-3", "prod"},
					},
					GroupBy: chronograf.GroupBy{
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
				Every: "30s",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,

					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{
							To: []string{},
						},
					},
				},
				Message: "message",
				Trigger: "threshold",
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90",
				},
				Name: "name",
			},
		},
		{
			name: "Test haproxy string comparison",
			script: `var db = 'influxdb'

		var rp = 'autogen'

		var measurement = 'haproxy'

		var groupBy = ['pxname']

		var whereFilter = lambda: TRUE

		var period = 10s

		var every = 10s

		var name = 'haproxy'

		var idVar = name + ':{{.Group}}'

		var message = 'Haproxy monitor : {{.ID}} : {{ index .Tags "server" }} : {{ index .Tags "pxname" }} is {{ .Level }} '

		var idTag = 'alertID'

		var levelTag = 'level'

		var messageField = 'message'

		var durationField = 'duration'

		var outputDB = 'chronograf'

		var outputRP = 'autogen'

		var outputMeasurement = 'alerts'

		var triggerType = 'threshold'

		var details = 'Email template'

		var crit = 'DOWN'

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
		    |last('status')
		        .as('value')

		var trigger = data
		    |alert()
		        .crit(lambda: "value" == crit)
		        .stateChangesOnly()
		        .message(message)
		        .id(idVar)
		        .idTag(idTag)
		        .levelTag(levelTag)
		        .messageField(messageField)
		        .durationField(durationField)
		        .details(details)
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
			want: chronograf.AlertRule{
				Name:    "haproxy",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "equal to",
					Value:    "DOWN",
				},
				Every:   "10s",
				Message: `Haproxy monitor : {{.ID}} : {{ index .Tags "server" }} : {{ index .Tags "pxname" }} is {{ .Level }} `,
				Details: "Email template",
				Query: &chronograf.QueryConfig{
					Database:        "influxdb",
					RetentionPolicy: "autogen",
					Measurement:     "haproxy",
					Fields: []chronograf.Field{
						{
							Value: "last",
							Args: []chronograf.Field{
								{
									Value: "status",
									Type:  "field",
								},
							},
							Type: "func",
						},
					},
					GroupBy: chronograf.GroupBy{
						Time: "10s",
						Tags: []string{"pxname"},
					},
					AreTagsAccepted: false,
				},
			},
		},
		{
			name: "Test haproxy",
			script: `var db = 'influxdb'

		var rp = 'autogen'

		var measurement = 'haproxy'

		var groupBy = ['pxname']

		var whereFilter = lambda: TRUE

		var period = 10s

		var every = 10s

		var name = 'haproxy'

		var idVar = name + ':{{.Group}}'

		var message = 'Haproxy monitor : {{.ID}} : {{ index .Tags "server" }} : {{ index .Tags "pxname" }} is {{ .Level }} '

		var idTag = 'alertID'

		var levelTag = 'level'

		var messageField = 'message'

		var durationField = 'duration'

		var outputDB = 'chronograf'

		var outputRP = 'autogen'

		var outputMeasurement = 'alerts'

		var triggerType = 'threshold'

		var details = 'Email template'

		var crit = 'DOWN'

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
		    |last('status')
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
			want: chronograf.AlertRule{
				Name:    "haproxy",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "DOWN",
				},
				Every:   "10s",
				Message: `Haproxy monitor : {{.ID}} : {{ index .Tags "server" }} : {{ index .Tags "pxname" }} is {{ .Level }} `,
				Details: "Email template",
				Query: &chronograf.QueryConfig{
					Database:        "influxdb",
					RetentionPolicy: "autogen",
					Measurement:     "haproxy",
					Fields: []chronograf.Field{
						{
							Value: "last",
							Args: []chronograf.Field{
								{
									Value: "status",
									Type:  "field",
								},
							},
							Type: "func",
						},
					},
					GroupBy: chronograf.GroupBy{
						Time: "10s",
						Tags: []string{"pxname"},
					},
					AreTagsAccepted: false,
				},
			},
		},
		{
			name: "Test valid template alert with detail",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90",
				},
				Every:   "30s",
				Message: "message",
				Details: "details",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
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
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test valid threshold inside range",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator:   "inside range",
					Value:      "90",
					RangeValue: "100",
				},
				Every:   "30s",
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
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
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test valid threshold outside range",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator:   "outside range",
					Value:      "90",
					RangeValue: "100",
				},
				Every:   "30s",
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
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
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test threshold no aggregate",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90",
				},
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "usage_user",
							Type:  "field",
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
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test relative alert",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "relative",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Change:   "% change",
					Shift:    "1m0s",
					Operator: "greater than",
					Value:    "90",
				},
				Every:   "30s",
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
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
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test relative change",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "relative",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Change:   "change",
					Shift:    "1m0s",
					Operator: "greater than",
					Value:    "90",
				},
				Every:   "30s",
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						{
							Value: "mean",
							Args: []chronograf.Field{
								{
									Value: "usage_user",
									Type:  "field",
								},
							},
							Type: "func",
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
						Time: "10m0s",
						Tags: []string{"host", "cluster_id"},
					},
					AreTagsAccepted: true,
				},
			},
		},
		{
			name: "Test deadman",
			script: `var db = 'telegraf'

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
			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "deadman",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
					Slack: []*chronograf.Slack{
						{},
					},
					VictorOps: []*chronograf.VictorOps{
						{},
					},
					Email: []*chronograf.Email{
						{To: []string{}},
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Period: "10m0s",
				},
				Message: "message",
				Query: &chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
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
				},
			},
		},
		{
			name: "Test threshold lambda",
			script: `var db = '_internal'

var rp = 'monitor'

var measurement = 'cq'

var groupBy = []

var whereFilter = lambda: TRUE

var name = 'rule 1'

var idVar = name + ':{{.Group}}'

var message = ''

var idTag = 'alertID'

var levelTag = 'level'

var messageField = 'message'

var durationField = 'duration'

var outputDB = 'chronograf'

var outputRP = 'autogen'

var outputMeasurement = 'alerts'

var triggerType = 'threshold'

var crit = 90000

var data = stream
    |from()
        .database(db)
        .retentionPolicy(rp)
        .measurement(measurement)
        .groupBy(groupBy)
        .where(whereFilter)
    |eval(lambda: "queryOk")
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
			want: chronograf.AlertRule{
				Name:    "rule 1",
				Trigger: "threshold",
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90000",
				},
				Every:   "",
				Message: "",
				Details: "",
				AlertNodes: chronograf.AlertNodes{
					IsStateChangesOnly: true,
				},
				Query: &chronograf.QueryConfig{
					Database:        "_internal",
					RetentionPolicy: "monitor",
					Measurement:     "cq",
					Fields: []chronograf.Field{
						{
							Value: "queryOk",
							Type:  "field",
						},
					},
					GroupBy: chronograf.GroupBy{
						Tags: []string{},
					},
					AreTagsAccepted: false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Reverse(tt.script)
			if (err != nil) != tt.wantErr {
				t.Errorf("reverse error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("reverse = %s", cmp.Diff(got, tt.want))
				if tt.want.Query != nil {
					if got.Query == nil {
						t.Errorf("reverse = got nil QueryConfig")
					} else if !cmp.Equal(*got.Query, *tt.want.Query) {
						t.Errorf("reverse = QueryConfig not equal %s", cmp.Diff(*got.Query, *tt.want.Query))
					}
				}
			}
		})
	}
}
