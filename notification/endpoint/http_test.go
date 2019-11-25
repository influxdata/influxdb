package endpoint_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
)

func TestHTTP_GenerateFlux(t *testing.T) {
	want := `package main
// http_test
import "influxdata/influxdb/monitor"
import "http"
import "csv"
import "json"

headers = {"Content-Type": "application/json"}
http_endpoint = http.endpoint(url: "http://localhost:7777")
data = "#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0"
csvTable = csv.from(csv: data)

csvTable
	|> monitor.notify(endpoint: http_endpoint(mapFn: (r) => {
		body = {r with _version: 1}

		return {headers: headers, data: json.encode(v: body)}
	}))`

	ne := &endpoint.HTTP{
		Base: endpoint.Base{
			Name: "http_test",
		},
		URL: "http://localhost:7777",
	}

	got, err := ne.GenerateTestFlux()

	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("\nExpected:\n\n%s\n\nGot:\n\n%s\n\n", want, got)
	}
}

func TestHTTP_GenerateFlux_basicAuth(t *testing.T) {
	want := `package main
// http_test_basic_auth
import "influxdata/influxdb/monitor"
import "http"
import "csv"
import "json"
import "influxdata/influxdb/secrets"

headers = {"Content-Type": "application/json", "Authorization": http.basicAuth(u: secrets.get(key: "000000000000000e-username"), p: secrets.get(key: "000000000000000e-password"))}
http_endpoint = http.endpoint(url: "http://localhost:7777")
data = "#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0"
csvTable = csv.from(csv: data)

csvTable
	|> monitor.notify(endpoint: http_endpoint(mapFn: (r) => {
		body = {r with _version: 1}

		return {headers: headers, data: json.encode(v: body)}
	}))`

	ne := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   2,
			Name: "http_test_basic_auth",
		},
		URL:        "http://localhost:7777",
		AuthMethod: "basic",
		Username: influxdb.SecretField{
			Key: "000000000000000e-username",
		},
		Password: influxdb.SecretField{
			Key: "000000000000000e-password",
		},
	}

	got, err := ne.GenerateTestFlux()

	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("\nExpected:\n\n%s\n\nGot:\n\n%s\n\n", want, got)
	}
}

func TestHTTP_GenerateFlux_bearer(t *testing.T) {
	want := `package main
// http_test_bearer
import "influxdata/influxdb/monitor"
import "http"
import "csv"
import "json"
import "influxdata/influxdb/secrets"

headers = {"Content-Type": "application/json", "Authorization": "Bearer " + secrets.get(key: "000000000000000e-token")}
http_endpoint = http.endpoint(url: "http://localhost:7777")
data = "#group,false,false,false,false,true,false,false
#datatype,string,long,string,string,string,string,long
#default,_result,,,,,,
,result,table,name,id,_measurement,retentionPolicy,retentionPeriod
,,0,telegraf,id1,m1,,0"
csvTable = csv.from(csv: data)

csvTable
	|> monitor.notify(endpoint: http_endpoint(mapFn: (r) => {
		body = {r with _version: 1}

		return {headers: headers, data: json.encode(v: body)}
	}))`

	ne := &endpoint.HTTP{
		Base: endpoint.Base{
			ID:   2,
			Name: "http_test_bearer",
		},
		URL:        "http://localhost:7777",
		AuthMethod: "bearer",
		Token: influxdb.SecretField{
			Key: "000000000000000e-token",
		},
	}

	got, err := ne.GenerateTestFlux()

	if err != nil {
		t.Fatal(err)
	}

	if got != want {
		t.Errorf("\nExpected:\n\n%s\n\nGot:\n\n%s\n\n", want, got)
	}
}
