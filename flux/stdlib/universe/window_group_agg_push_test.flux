package universe_test

import "testing"
import "testing/expect"
import "planner"
import "csv"

option now = () => (2030-01-01T00:00:00Z)

input = "
#datatype,string,long,dateTime:RFC3339,string,string,string,double
#group,false,false,false,true,true,true,false
#default,_result,,,,,,
,result,table,_time,_measurement,host,_field,_value
,,0,2018-05-22T19:53:26Z,system,hostA,load1,1.83
,,0,2018-05-22T19:53:36Z,system,hostA,load1,1.72
,,0,2018-05-22T19:53:37Z,system,hostA,load1,1.77
,,0,2018-05-22T19:53:56Z,system,hostA,load1,1.63
,,0,2018-05-22T19:54:06Z,system,hostA,load1,1.91
,,0,2018-05-22T19:54:16Z,system,hostA,load1,1.84

,,1,2018-05-22T19:53:26Z,system,hostB,load3,1.98
,,1,2018-05-22T19:53:36Z,system,hostB,load3,1.97
,,1,2018-05-22T19:53:46Z,system,hostB,load3,1.97
,,1,2018-05-22T19:53:56Z,system,hostB,load3,1.96
,,1,2018-05-22T19:54:06Z,system,hostB,load3,1.98
,,1,2018-05-22T19:54:16Z,system,hostB,load3,1.97

,,2,2018-05-22T19:53:26Z,system,hostC,load5,1.95
,,2,2018-05-22T19:53:36Z,system,hostC,load5,1.92
,,2,2018-05-22T19:53:41Z,system,hostC,load5,1.91
,,2,2018-05-22T19:53:46Z,system,hostC,load5,1.92
,,2,2018-05-22T19:53:56Z,system,hostC,load5,1.89
,,2,2018-05-22T19:54:16Z,system,hostC,load5,1.93
"

output = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,long
#group,false,false,true,true,true,false
#default,_result,,,,,
,result,table,_start,_stop,host,_value
,,0,2018-05-22T19:53:20Z,2018-05-22T19:53:40Z,hostA,3
,,1,2018-05-22T19:53:20Z,2018-05-22T19:53:40Z,hostB,2
,,2,2018-05-22T19:53:20Z,2018-05-22T19:53:40Z,hostC,2
,,3,2018-05-22T19:53:40Z,2018-05-22T19:54:00Z,hostA,1
,,4,2018-05-22T19:53:40Z,2018-05-22T19:54:00Z,hostB,2
,,5,2018-05-22T19:53:40Z,2018-05-22T19:54:00Z,hostC,3
,,6,2018-05-22T19:54:00Z,2018-05-22T19:54:20Z,hostA,2
,,7,2018-05-22T19:54:00Z,2018-05-22T19:54:20Z,hostB,2
,,8,2018-05-22T19:54:00Z,2018-05-22T19:54:20Z,hostC,1
"

testcase group_window_agg_pushdown {
    expect.planner(rules: ["GroupWindowAggregateTransposeRule": 1])

    want = csv.from(csv: output)
    result = testing.loadStorage(csv: input)
        |> range(start: -100y)
        |> group(columns: ["host"])
        |> window(every: 20s)
        |> count()

    testing.diff(want: want, got: result)
}
