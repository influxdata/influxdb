package kapacitor

import (
	"fmt"
	"testing"

	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestPipelineJSON(t *testing.T) {
	script := `var db = 'telegraf'

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
`

	want := `var alert4 = stream
    |from()
        .database('telegraf')
        .retentionPolicy('autogen')
        .measurement('cpu')
        .where(lambda: "cpu" == 'cpu_total' AND "host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')
        .groupBy('host', 'cluster_id')
    |window()
        .period(10m)
        .every(30s)
        .align()
    |mean('usage_user')
        .as('value')
    |alert()
        .id('name:{{.Group}}')
        .message('message')
        .details('{{ json . }}')
        .crit(lambda: "value" > 90)
        .history(21)
        .levelTag('level')
        .messageField('message')
        .durationField('duration')
        .idTag('alertID')
        .stateChangesOnly()
        .email()
        .victorOps()
        .slack()

alert4
    |httpOut('output')

alert4
    |influxDBOut()
        .database('chronograf')
        .retentionPolicy('autogen')
        .measurement('alerts')
        .buffer(1000)
        .flushInterval(10s)
        .create()
        .tag('alertName', 'name')
        .tag('triggerType', 'threshold')
`

	octets, err := MarshalTICK(script)
	if err != nil {
		t.Fatalf("MarshalTICK unexpected error %v", err)
	}

	got, err := UnmarshalTICK(octets)
	if err != nil {
		t.Fatalf("UnmarshalTICK unexpected error %v", err)
	}

	if got != want {
		fmt.Println(got)
		diff := diffmatchpatch.New()
		delta := diff.DiffMain(want, got, true)
		t.Errorf("%s", diff.DiffPrettyText(delta))
	}
}
func TestPipelineJSONDeadman(t *testing.T) {
	script := `var db = 'telegraf'

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
`

	wantA := `var from1 = stream
    |from()
        .database('telegraf')
        .retentionPolicy('autogen')
        .measurement('cpu')
        .where(lambda: "cpu" == 'cpu_total' AND "host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')
        .groupBy('host', 'cluster_id')

var alert5 = from1
    |stats(10m)
        .align()
    |derivative('emitted')
        .as('emitted')
        .unit(10m)
        .nonNegative()
    |alert()
        .id('name:{{.Group}}')
        .message('message')
        .details('{{ json . }}')
        .crit(lambda: "emitted" <= 0.0)
        .history(21)
        .levelTag('level')
        .messageField('message')
        .durationField('duration')
        .idTag('alertID')
        .stateChangesOnly()
        .email()
        .victorOps()
        .slack()

alert5
    |httpOut('output')

alert5
    |eval(lambda: "emitted")
        .as('value')
        .tags()
        .keep('value', 'message', 'duration')
    |influxDBOut()
        .database('chronograf')
        .retentionPolicy('autogen')
        .measurement('alerts')
        .buffer(1000)
        .flushInterval(10s)
        .create()
        .tag('alertName', 'name')
        .tag('triggerType', 'deadman')
`

	wantB := `var from1 = stream
    |from()
        .database('telegraf')
        .retentionPolicy('autogen')
        .measurement('cpu')
        .where(lambda: "cpu" == 'cpu_total' AND "host" == 'acc-0eabc309-eu-west-1-data-3' OR "host" == 'prod')
        .groupBy('host', 'cluster_id')

var alert5 = from1
    |stats(10m)
        .align()
    |derivative('emitted')
        .as('emitted')
        .unit(10m)
        .nonNegative()
    |alert()
        .id('name:{{.Group}}')
        .message('message')
        .details('{{ json . }}')
        .crit(lambda: "emitted" <= 0.0)
        .history(21)
        .levelTag('level')
        .messageField('message')
        .durationField('duration')
        .idTag('alertID')
        .stateChangesOnly()
        .email()
        .victorOps()
        .slack()

alert5
    |eval(lambda: "emitted")
        .as('value')
        .tags()
        .keep('value', 'message', 'duration')
    |influxDBOut()
        .database('chronograf')
        .retentionPolicy('autogen')
        .measurement('alerts')
        .buffer(1000)
        .flushInterval(10s)
        .create()
        .tag('alertName', 'name')
        .tag('triggerType', 'deadman')

alert5
    |httpOut('output')
`

	octets, err := MarshalTICK(script)
	if err != nil {
		t.Fatalf("MarshalTICK unexpected error %v", err)
	}
	got, err := UnmarshalTICK(octets)
	if err != nil {
		t.Fatalf("UnmarshalTICK unexpected error %v", err)
	}

	if got != wantA && got != wantB {
		want := wantA
		fmt.Println("got")
		fmt.Println(got)
		fmt.Println("want")
		fmt.Println(want)
		diff := diffmatchpatch.New()
		delta := diff.DiffMain(want, got, true)
		t.Errorf("%s", diff.DiffPrettyText(delta))
	}
}
