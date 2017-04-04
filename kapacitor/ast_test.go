package kapacitor

import (
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
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
													.email('howdy@howdy.com')
													`),

			want: chronograf.AlertRule{
				Name:    "name",
				Trigger: "threshold",
				Alerts:  []string{"victorops", "smtp", "slack"},
				AlertNodes: []chronograf.KapacitorNode{
					{
						Name: "victorops",
					},
					{
						Name: "smtp",
						Args: []string{"howdy@howdy.com"},
					},
					{
						Name: "slack",
					},
				},
				TriggerValues: chronograf.TriggerValues{
					Operator: "greater than",
					Value:    "90",
				},
				Every:   "30s",
				Message: "message",
				Details: "details",
				Query: chronograf.QueryConfig{
					Database:        "telegraf",
					RetentionPolicy: "autogen",
					Measurement:     "cpu",
					Fields: []chronograf.Field{
						{
							Field: "usage_user",
							Funcs: []string{
								"mean",
							},
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
				Query: chronograf.QueryConfig{
					Database:        "telegraf",
					Measurement:     "cpu",
					RetentionPolicy: "autogen",
					Fields: []chronograf.Field{
						chronograf.Field{
							Field: "usage_user",
							Funcs: []string{"mean"},
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
				Alerts: []string{
					"victorops",
					"smtp",
					"slack",
				},
				AlertNodes: []chronograf.KapacitorNode{
					chronograf.KapacitorNode{
						Name: "victorops",
					},
					chronograf.KapacitorNode{
						Name: "smtp",
					},
					chronograf.KapacitorNode{
						Name: "slack",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Reverse(tt.script)
			if (err != nil) != tt.wantErr {
				t.Errorf("Reverse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reverse() = \n%#v\n, want \n%#v\n", got, tt.want)
			}
		})
	}
}
