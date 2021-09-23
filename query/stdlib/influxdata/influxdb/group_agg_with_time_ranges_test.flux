// TODO(whb): These tests should get ported to the flux repo and removed here when they are included with a flux release
// that InfluxDB uses to remove the redundancy.

package influxdb_test

import "csv"
import "testing"
import "testing/expect"

option now = () => 2030-01-01T00:00:00Z

input = "
#datatype,string,long,dateTime:RFC3339,long,string,string,string,string
#group,false,false,false,false,true,true,true,true
#default,_result,,,,,,,
,result,table,_time,_value,_field,_measurement,host,name
,,0,2018-05-22T19:53:26Z,15204688,io_time,diskio,host.local,disk0
,,0,2018-05-22T19:53:36Z,15204894,io_time,diskio,host.local,disk0
,,0,2018-05-22T19:53:46Z,15205102,io_time,diskio,host.local,disk0
,,0,2018-05-22T19:53:56Z,15205226,io_time,diskio,host.local,disk0
,,0,2018-05-22T19:54:06Z,15205499,io_time,diskio,host.local,disk0
,,0,2018-05-22T19:54:16Z,15205755,io_time,diskio,host.local,disk0
,,1,2018-05-23T19:53:26Z,648,io_time,diskio,host.local,disk2
,,1,2018-05-23T19:53:36Z,648,io_time,diskio,host.local,disk2
,,1,2018-05-23T19:53:46Z,648,io_time,diskio,host.local,disk2
,,1,2018-05-23T19:53:56Z,648,io_time,diskio,host.local,disk2
,,1,2018-05-23T19:54:06Z,648,io_time,diskio,host.local,disk2
,,1,2018-05-23T19:54:16Z,648,io_time,diskio,host.local,disk2
"

// Basic matching of series by the predicate - verifies that non-matching series do not get included in the result. Time
// range covers the entire dataset.
testcase group_agg_with_time_ranges_filters {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-05-22T00:00:00Z, stop: 2018-05-24T00:00:00Z)
        |> filter(fn: (r) => r["_measurement"] == "diskio" and r["name"] == "disk0")
        |> group(columns: ["_measurement"], mode:"by")
        |> last()

outData = "#group,false,false,true,true,false,false,false,true,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host,name
,,0,2018-05-22T00:00:00Z,2018-05-24T00:00:00Z,2018-05-22T19:54:16Z,15205755,io_time,diskio,host.local,disk0"

    want = csv.from(csv: outData)

    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    testing.diff(got, want)
}

// Multiple series match the predicate - verifies that series without data in the requested time range do not get
// included in the result.
testcase group_agg_with_time_ranges_multiple_series_match_filter {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-05-23T19:53:36Z, stop: 2018-05-23T19:54:07Z)
        |> filter(fn: (r) => r["_measurement"] == "diskio")
        |> group(columns: ["_measurement"], mode:"by")
        |> first()

outData = "#group,false,false,true,true,false,false,false,true,false,false
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host,name
,,0,2018-05-23T19:53:36Z,2018-05-23T19:54:07Z,2018-05-23T19:53:36Z,648,io_time,diskio,host.local,disk2"

    want = csv.from(csv: outData)

    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    testing.diff(got, want)
}

// Same as above but with "count" instead of "first".
testcase group_agg_with_time_ranges_multiple_series_match_filter_count {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-05-23T19:53:36Z, stop: 2018-05-23T19:54:07Z)
        |> filter(fn: (r) => r["_measurement"] == "diskio")
        |> group(columns: ["_measurement"], mode:"by")
        |> count()

outData = "#group,false,false,true,true,false,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,string
#default,_result,,,,,
,result,table,_start,_stop,_value,_measurement
,,0,2018-05-23T19:53:36Z,2018-05-23T19:54:07Z,4,diskio"

    want = csv.from(csv: outData)

    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    testing.diff(got, want)
}
