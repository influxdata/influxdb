package universe_test

import "testing"
import "testing/expect"
import "planner"
import "csv"

input = "
#group,false,false,true,true,false,false,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,meter
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-04-11T07:00:00Z,0,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-04-23T07:00:00Z,64,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-05-22T07:00:00Z,759,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-06-24T07:00:00Z,1234,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-07-24T07:00:00Z,1503,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-08-22T07:00:00Z,1707,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-09-23T07:00:00Z,1874,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-10-23T07:00:00Z,2086,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-11-21T08:00:00Z,2187,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2019-12-24T08:00:00Z,1851,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-01-24T08:00:00Z,1391,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-02-24T08:00:00Z,1221,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-03-25T07:00:00Z,0,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-04-23T07:00:00Z,447,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-05-22T07:00:00Z,868,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-06-23T07:00:00Z,1321,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-07-23T07:00:00Z,1453,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-08-21T07:00:00Z,1332,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-09-23T07:00:00Z,1312,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-10-22T07:00:00Z,1261,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-11-20T08:00:00Z,933,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2020-12-23T08:00:00Z,233,bank,pge_bill,35632393IN
,,0,2017-02-16T20:30:31.713576368Z,2021-02-16T20:30:31.713576368Z,2021-01-26T08:00:00Z,-1099,bank,pge_bill,35632393IN
"

testcase bare_count {
    expect.planner(rules: ["PushDownBareAggregateRule": 1])

    want = csv.from(
        csv: "
#datatype,string,long,long
#group,false,false,false
#default,_result,,
,result,table,_value
,,0,23
",
    )
    result = testing.loadStorage(csv: input)
        |> range(start: -3y)
        |> count()
        |> keep(columns: ["_value"])

        testing.diff(want: want, got: result)
}

testcase bare_last {
    expect.planner(rules: ["PushDownBareAggregateRule": 1])

    want = csv.from(
        csv: "
#group,false,false,false,false,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2021-01-26T08:00:00Z,-1099,bank,pge_bill
",
    )
    result = testing.loadStorage(csv: input)
        |> range(start: -3y)
        |> last()
        |> keep(columns: ["_time", "_value", "_field", "_measurement"])

    testing.diff(want: want, got: result)
}
