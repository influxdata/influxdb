package endpoint_test

import (
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/influxdb/notification/endpoint"
)

func TestSlack_GenerateFlux(t *testing.T) {
	want := `package main
// test
import "influxdata/influxdb/monitor"
import "slack"
import "csv"

slack_endpoint = slack.endpoint(url: "dummyURL")
data = "#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0"
csvTable = csv.from(csv: data)

csvTable
	|> monitor.notify(data: {}, endpoint: slack_endpoint(mapFn: (r) =>
		({channel: "slack-test", text: "Your endpoint named \"test\" is working", color: "good"})))`

	ne := &endpoint.Slack{
		Base: endpoint.Base{
			Name: "test",
		},
		URL:         "dummyURL",
		TestChannel: "slack-test",
	}

	got, err := ne.GenerateTestFlux()

	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(got, want))
	}
}
