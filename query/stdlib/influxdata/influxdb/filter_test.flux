package influxdb_test

import "csv"
import "testing"
import "testing/expect"

option now = () => (2030-01-01T00:00:00Z)

input = "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.63
,,1,2018-05-22T19:53:26Z,system,host.local,load3,1.72
,,2,2018-05-22T19:53:26Z,system,host.local,load4,1.77
,,2,2018-05-22T19:53:36Z,system,host.local,load4,1.78
,,2,2018-05-22T19:53:46Z,system,host.local,load4,1.77
"

testcase filter {
    expect.planner(rules: [
        "influxdata/influxdb.FromStorageRule": 1,
        "PushDownRangeRule": 1,
        "PushDownFilterRule": 1,
    ])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.63
")

    got = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> filter(fn: (r) => r._measurement == "system" and r._field == "load1")
        |> drop(columns: ["_start", "_stop"])
    testing.diff(want, got)
}


input_issue_4804 = "#datatype,string,long,dateTime:RFC3339,string,string,string,boolean
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,true
,,0,2018-05-22T19:53:36Z,system,host.local,load1,false
,,1,2018-05-22T19:53:26Z,system,host.local,load3,false
,,2,2018-05-22T19:53:26Z,system,host.local,load4,true
"

testcase flux_issue_4804 {
    expect.planner(rules: [
        "influxdata/influxdb.FromStorageRule": 1,
        "PushDownRangeRule": 1,
        "PushDownFilterRule": 1,
    ])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,boolean
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,true
,,1,2018-05-22T19:53:26Z,system,host.local,load3,false
")

    got = csv.from(csv: input_issue_4804)
        |> testing.load()
        |> range(start: -100y)
        |> filter(fn: (r) => ((r["_field"] == "load1" and r["_value"] == true) or (r["_field"] == "load3" and r["_value"] == false)))
        |> drop(columns: ["_start", "_stop"])
    testing.diff(want, got)
}
