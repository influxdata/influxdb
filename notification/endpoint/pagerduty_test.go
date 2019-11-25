package endpoint_test

import (
	"testing"

	"github.com/andreyvit/diff"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
)

func TestPagerDuty_GenerateFlux(t *testing.T) {
	want := `package main
// pager_duty_test
import "influxdata/influxdb/monitor"
import "pagerduty"
import "csv"
import "influxdata/influxdb/secrets"

pagerduty_endpoint = pagerduty.endpoint()
data = "#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0"
csvTable = csv.from(csv: data)

csvTable
	|> monitor.notify(endpoint: pagerduty_endpoint(mapFn: (r) =>
		({
			routingKey: "pagerduty_token",
			clientURL: "dummyURL",
			severity: "info",
			eventAction: "trigger",
			source: "pager_duty_test",
			summary: "PagerDuty connection successful",
		})))`

	e := &endpoint.PagerDuty{
		Base: endpoint.Base{
			ID:   2,
			Name: "pager_duty_test",
		},
		ClientURL: "dummyURL",
		RoutingKey: influxdb.SecretField{
			Key: "pagerduty_token",
		},
	}

	value := "pagerduty_token"
	e.RoutingKey.Value = &value

	got, err := e.GenerateTestFlux()

	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("\n\nStrings do not match:\n\n%s", diff.LineDiff(got, want))
	}
}
