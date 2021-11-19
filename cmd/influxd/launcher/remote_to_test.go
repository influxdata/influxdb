package launcher_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/stretchr/testify/require"
)

func TestRemoteTo(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Boot 2 servers.
	l1 := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l1.ShutdownOrFail(t, ctx)
	l2 := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l2.ShutdownOrFail(t, ctx)

	// Run a flux script in the 1st server, writing data to the 2nd.
	q1 := fmt.Sprintf(`import "array"

option now = () => (2030-01-01T00:01:00Z)

rows = [
	{_time: now(), _measurement: "test", _field: "f", _value: 1.0},
	{_time: now(), _measurement: "test", _field: "v", _value: -123.0},
	{_time: now(), _measurement: "test2", _field: "f", _value: 0.03}
]

array.from(rows) |> to(bucket: "%s", host: "%s", token: "%s", org: "%s")
`, l2.Bucket.Name, l2.URL().String(), l2.Auth.Token, l2.Org.Name)
	_ = l1.FluxQueryOrFail(t, l1.Org, l1.Auth.Token, q1)

	// Query the 2nd server and check that the points landed.
	q2 := fmt.Sprintf(`from(bucket:"%s")
	|> range(start: 2030-01-01T00:00:00Z, stop: 2030-01-02T00:00:00Z)
	|> keep(columns: ["_measurement", "_field", "_value"])
`, l2.Bucket.Name)
	exp := `,result,table,_value,_field,_measurement` + "\r\n" +
		`,_result,0,1,f,test` + "\r\n" +
		`,_result,1,0.03,f,test2` + "\r\n" +
		`,_result,2,-123,v,test` + "\r\n\r\n"
	res := l2.FluxQueryOrFail(t, l2.Org, l2.Auth.Token, q2)
	require.Equal(t, exp, res)
}

func TestRemoteTo_Experimental(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Boot 2 servers.
	l1 := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l1.ShutdownOrFail(t, ctx)
	l2 := launcher.RunAndSetupNewLauncherOrFail(ctx, t)
	defer l2.ShutdownOrFail(t, ctx)

	// Run a flux script in the 1st server, writing data to the 2nd.
	q1 := fmt.Sprintf(`import "array"
import "experimental"

option now = () => (2030-01-01T00:01:00Z)

testRows = [
	{_time: now(), _field: "f", _value: 1.0},
	{_time: now(), _field: "v", _value: -123.0},
]

test2Rows = [
	{_time: now(), _field: "f", _value: 0.03}
]

testTable = array.from(rows: testRows)
	|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> map(fn: (r) => ({r with _measurement: "test"}))

test2Table = array.from(rows: test2Rows)
	|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
	|> map(fn: (r) => ({r with _measurement: "test2"}))

union(tables: [testTable, test2Table]) |> group(columns: ["_measurement"])
	|> experimental.to(bucket: "%s", host: "%s", token: "%s", org: "%s")
`, l2.Bucket.Name, l2.URL().String(), l2.Auth.Token, l2.Org.Name)
	_ = l1.FluxQueryOrFail(t, l1.Org, l1.Auth.Token, q1)

	// Query the 2nd server and check that the points landed.
	q2 := fmt.Sprintf(`from(bucket:"%s")
	|> range(start: 2030-01-01T00:00:00Z, stop: 2030-01-02T00:00:00Z)
	|> keep(columns: ["_measurement", "_field", "_value"])
`, l2.Bucket.Name)
	exp := `,result,table,_value,_field,_measurement` + "\r\n" +
		`,_result,0,1,f,test` + "\r\n" +
		`,_result,1,0.03,f,test2` + "\r\n" +
		`,_result,2,-123,v,test` + "\r\n\r\n"
	res := l2.FluxQueryOrFail(t, l2.Org, l2.Auth.Token, q2)
	require.Equal(t, exp, res)
}
