package influxdb_test

import "csv"
import "testing"

option now = () => 2030-01-01T00:00:00Z

input = "
#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84

,,1,2018-05-22T19:53:26Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:53:36Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:46Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:56Z,sys,host.local,load3,1.96
,,1,2018-05-22T19:54:06Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:54:16Z,sys,host.local,load3,1.97

,,2,2018-05-22T19:53:26Z,system,host.local,load5,1.95
,,2,2018-05-22T19:53:36Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:46Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:56Z,system,host.local,load5,1.89
,,2,2018-05-22T19:54:06Z,system,host.local,load5,1.94
,,2,2018-05-22T19:54:16Z,system,host.local,load5,1.93

,,3,2018-05-22T19:53:26Z,var,host.local,load3,91.98
,,3,2018-05-22T19:53:36Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:46Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:56Z,var,host.local,load3,91.96
,,3,2018-05-22T19:54:06Z,var,host.local,load3,91.98
,,3,2018-05-22T19:54:16Z,var,host.local,load3,91.97

,,4,2018-05-22T19:53:26Z,swap,host.global,used_percent,82.98
,,4,2018-05-22T19:53:36Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:46Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:56Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:06Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:16Z,swap,host.global,used_percent,82.64
"

testcase multi_measure {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-01-01T00:00:00Z, stop: 2019-01-01T00:00:00Z)
        |> filter(fn: (r) => r["_measurement"] == "system" or r["_measurement"] == "sys") 
        |> filter(fn: (r) => r["_field"] == "load1" or r["_field"] == "load3")
        |> drop(columns: ["_start", "_stop"])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84
,,1,2018-05-22T19:53:26Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:53:36Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:46Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:56Z,sys,host.local,load3,1.96
,,1,2018-05-22T19:54:06Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:54:16Z,sys,host.local,load3,1.97
")

    testing.diff(got, want)
}

testcase multi_measure_match_all {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-01-01T00:00:00Z, stop: 2019-01-01T00:00:00Z)
        |> filter(fn: (r) => r["_measurement"] == "system" or r["_measurement"] == "sys" or r["_measurement"] == "var" or r["_measurement"] == "swap") 
        |> filter(fn: (r) => r["_field"] == "load1" or r["_field"] == "load3" or r["_field"] == "load5" or r["_field"] == "used_percent")
        |> drop(columns: ["_start", "_stop"])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84
,,1,2018-05-22T19:53:26Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:53:36Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:46Z,sys,host.local,load3,1.97
,,1,2018-05-22T19:53:56Z,sys,host.local,load3,1.96
,,1,2018-05-22T19:54:06Z,sys,host.local,load3,1.98
,,1,2018-05-22T19:54:16Z,sys,host.local,load3,1.97
,,2,2018-05-22T19:53:26Z,system,host.local,load5,1.95
,,2,2018-05-22T19:53:36Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:46Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:56Z,system,host.local,load5,1.89
,,2,2018-05-22T19:54:06Z,system,host.local,load5,1.94
,,2,2018-05-22T19:54:16Z,system,host.local,load5,1.93
,,3,2018-05-22T19:53:26Z,var,host.local,load3,91.98
,,3,2018-05-22T19:53:36Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:46Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:56Z,var,host.local,load3,91.96
,,3,2018-05-22T19:54:06Z,var,host.local,load3,91.98
,,3,2018-05-22T19:54:16Z,var,host.local,load3,91.97
,,4,2018-05-22T19:53:26Z,swap,host.global,used_percent,82.98
,,4,2018-05-22T19:53:36Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:46Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:56Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:06Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:16Z,swap,host.global,used_percent,82.64
")

    testing.diff(got, want)
}

testcase multi_measure_tag_filter {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-01-01T00:00:00Z, stop: 2019-01-01T00:00:00Z)
        |> filter(fn: (r) => r["_measurement"] == "system" or r["_measurement"] == "swap")
        |> filter(fn: (r) => r["_field"] == "load1" or r["_field"] == "load3" or r["_field"] == "used_percent")
        |> filter(fn: (r) => r["host"] == "host.local" or r["host"] == "host.global")
        |> drop(columns: ["_start", "_stop"])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84
,,4,2018-05-22T19:53:26Z,swap,host.global,used_percent,82.98
,,4,2018-05-22T19:53:36Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:46Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:56Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:06Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:16Z,swap,host.global,used_percent,82.64
")

    testing.diff(got, want)
}

testcase multi_measure_complex_or {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-01-01T00:00:00Z, stop: 2019-01-01T00:00:00Z)
        |> filter(fn: (r) => (r["_measurement"] == "system" or r["_measurement"] == "swap") or (r["_measurement"] == "var" and r["host"] == "host.local")) 
        |> drop(columns: ["_start", "_stop"])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,host.local,load1,1.83
,,0,2018-05-22T19:53:36Z,system,host.local,load1,1.72
,,0,2018-05-22T19:53:46Z,system,host.local,load1,1.74
,,0,2018-05-22T19:53:56Z,system,host.local,load1,1.63
,,0,2018-05-22T19:54:06Z,system,host.local,load1,1.91
,,0,2018-05-22T19:54:16Z,system,host.local,load1,1.84
,,2,2018-05-22T19:53:26Z,system,host.local,load5,1.95
,,2,2018-05-22T19:53:36Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:46Z,system,host.local,load5,1.92
,,2,2018-05-22T19:53:56Z,system,host.local,load5,1.89
,,2,2018-05-22T19:54:06Z,system,host.local,load5,1.94
,,2,2018-05-22T19:54:16Z,system,host.local,load5,1.93
,,4,2018-05-22T19:53:26Z,swap,host.global,used_percent,82.98
,,4,2018-05-22T19:53:36Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:46Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:56Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:06Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:16Z,swap,host.global,used_percent,82.64
,,3,2018-05-22T19:53:26Z,var,host.local,load3,91.98
,,3,2018-05-22T19:53:36Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:46Z,var,host.local,load3,91.97
,,3,2018-05-22T19:53:56Z,var,host.local,load3,91.96
,,3,2018-05-22T19:54:06Z,var,host.local,load3,91.98
,,3,2018-05-22T19:54:16Z,var,host.local,load3,91.97
")

    testing.diff(got, want)
}

testcase multi_measure_complex_and {
    got = testing.loadStorage(csv: input)
        |> range(start: 2018-01-01T00:00:00Z, stop: 2019-01-01T00:00:00Z)
        |> filter(fn: (r) => r["_measurement"] == "system" or r["_measurement"] == "swap") 
        |> filter(fn: (r) => r["_measurement"] == "swap" or r["_measurement"] == "var") 
        |> drop(columns: ["_start", "_stop"])

    want = csv.from(csv: "#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,4,2018-05-22T19:53:26Z,swap,host.global,used_percent,82.98
,,4,2018-05-22T19:53:36Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:46Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:53:56Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:06Z,swap,host.global,used_percent,82.59
,,4,2018-05-22T19:54:16Z,swap,host.global,used_percent,82.64
")

    testing.diff(got, want)
}