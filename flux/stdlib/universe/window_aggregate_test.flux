package universe_test

import "testing"
import "testing/expect"
import "planner"
import "csv"

option now = () => (2030-01-01T00:00:00Z)

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

testcase windowed_count {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,true,false
#default,_result,,,
,result,table,_start,_value
,,0,2019-01-01T00:00:00Z,10
,,1,2020-01-01T00:00:00Z,12
,,2,2021-01-01T00:00:00Z,1
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> count()
        |> keep(columns: ["_start", "_value"])

    testing.diff(want: want, got: result)
}

testcase windowed_sum {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,true,false
#default,_result,,,
,result,table,_start,_value
,,0,2019-01-01T00:00:00Z,13265.00
,,1,2020-01-01T00:00:00Z,11772.00
,,2,2021-01-01T00:00:00Z,-1099.00
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> sum()
        |> keep(columns: ["_start", "_value"])

    testing.diff(want: want, got: result)
}

testcase windowed_mean {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#datatype,string,long,dateTime:RFC3339,double
#group,false,false,true,false
#default,_result,,,
,result,table,_start,_value
,,0,2019-01-01T00:00:00Z,1326.50
,,1,2020-01-01T00:00:00Z,981.00
,,2,2021-01-01T00:00:00Z,-1099.00
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> mean()
        |> keep(columns: ["_start", "_value"])

    testing.diff(want: want, got: result)
}

testcase windowed_min {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#group,false,false,false,false,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2019-04-11T07:00:00Z,0,bank,pge_bill
,,0,2020-03-25T07:00:00Z,0,bank,pge_bill
,,0,2021-01-26T08:00:00Z,-1099,bank,pge_bill
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> min()
        |> keep(columns: ["_time", "_value", "_field", "_measurement"])
        |> sort(columns: ["_time"])

    testing.diff(want: want, got: result)
}

testcase windowed_max {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#group,false,false,false,false,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2019-11-21T08:00:00Z,2187,bank,pge_bill
,,0,2020-07-23T07:00:00Z,1453,bank,pge_bill
,,0,2021-01-26T08:00:00Z,-1099,bank,pge_bill
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> max()
        |> keep(columns: ["_time", "_value", "_field", "_measurement"])
        |> sort(columns: ["_time"])

    testing.diff(want: want, got: result)
}

testcase windowed_first {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#group,false,false,false,false,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2019-04-11T07:00:00Z,0,bank,pge_bill
,,0,2020-01-24T08:00:00Z,1391,bank,pge_bill
,,0,2021-01-26T08:00:00Z,-1099,bank,pge_bill
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> first()
        |> keep(columns: ["_time", "_value", "_field", "_measurement"])
        |> sort(columns: ["_time"])

    testing.diff(want: want, got: result)
}

testcase windowed_last {
    expect.planner(rules: ["PushDownWindowAggregateRule": 1])

    want = csv.from(
        csv: "
#group,false,false,false,false,true,true
#datatype,string,long,dateTime:RFC3339,double,string,string
#default,_result,,,,,
,result,table,_time,_value,_field,_measurement
,,0,2019-12-24T08:00:00Z,1851.000,bank,pge_bill
,,0,2020-12-23T08:00:00Z,233.000,bank,pge_bill
,,0,2021-01-26T08:00:00Z,-1099,bank,pge_bill
",
    )
    result = csv.from(csv: input)
        |> testing.load()
        |> range(start: -100y)
        |> window(every: 1y)
        |> last()
        |> keep(columns: ["_time", "_value", "_field", "_measurement"])
        |> sort(columns: ["_time"])

    testing.diff(want: want, got: result)
}
