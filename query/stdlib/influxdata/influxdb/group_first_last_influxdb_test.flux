package influxdb_test


import "array"
import "testing"
import "testing/expect"

// N.b. `inData` is what this is named in the testcases using extension.
// Apparently we can't shadow the name here when extending, else we get an error
// about reassignment.
input =
    array.from(
        rows: [
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:30Z,
                _value: 3,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:40Z,
                _value: 1,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:50Z,
                _value: 0,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:30Z,
                _value: 4,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:40Z,
                _value: 3,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:50Z,
                _value: 1,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:30Z,
                _value: 1,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:40Z,
                _value: 0,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:50Z,
                _value: 4,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:30Z,
                _value: 4,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:40Z,
                _value: 0,
            },
            {
                _field: "f0",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:50Z,
                _value: 4,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:30Z,
                _value: 0,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:40Z,
                _value: 0,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v0",
                _time: 2021-07-06T23:06:50Z,
                _value: 0,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:30Z,
                _value: 0,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:40Z,
                _value: 4,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v0",
                t1: "t1v1",
                _time: 2021-07-06T23:06:50Z,
                _value: 3,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:30Z,
                _value: 3,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:40Z,
                _value: 2,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v0",
                _time: 2021-07-06T23:06:50Z,
                _value: 1,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:30Z,
                _value: 1,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:40Z,
                _value: 0,
            },
            {
                _field: "f1",
                _measurement: "m0",
                t0: "t0v1",
                t1: "t1v1",
                _time: 2021-07-06T23:06:50Z,
                _value: 2,
            },
        ],
    )
        |> group(columns: ["_measurement", "_field", "t0", "t1"])

// FIXME: want to extend `flux/planner/group_first_last_test.group_one_tag_first` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_one_tag_first {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v0",
                    "t1": "t1v0",
                    "_value": 3,
                    _time: 2021-07-06T23:06:30Z,
                },
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v1",
                    "t1": "t1v0",
                    "_value": 1,
                    _time: 2021-07-06T23:06:30Z,
                },
            ],
        )
            |> group(columns: ["t0"])
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> group(columns: ["t0"])
            |> first()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

// FIXME: want to extend `flux/planner/group_first_last_test.group_all_filter_field_first` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_all_filter_field_first {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v0",
                    "t1": "t1v0",
                    "_value": 3,
                    _time: 2021-07-06T23:06:30Z,
                },
            ],
        )
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> filter(fn: (r) => r._field == "f0")
            |> group()
            |> first()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

// FIXME: want to extend `flux/planner/group_first_last_test.group_one_tag_filter_field_first` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_one_tag_filter_field_first {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v0",
                    "t1": "t1v0",
                    "_value": 3,
                    _time: 2021-07-06T23:06:30Z,
                },
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v1",
                    "t1": "t1v0",
                    "_value": 1,
                    _time: 2021-07-06T23:06:30Z,
                },
            ],
        )
            |> group(columns: ["t0"])
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> filter(fn: (r) => r._field == "f0")
            |> group(columns: ["t0"])
            |> first()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

testcase
push_down_group_two_tag_filter_field_first
extends
"flux/planner/group_first_last_test.group_two_tag_filter_field_first"
{
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

// Group + last tests
// FIXME: want to extend `flux/planner/group_first_last_test.group_one_tag_last` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_one_tag_last {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f1",
                    "t0": "t0v0",
                    "t1": "t1v1",
                    _time: 2021-07-06T23:06:50Z,
                    _value: 3,
                },
                {
                    _measurement: "m0",
                    _field: "f1",
                    "t0": "t0v1",
                    "t1": "t1v1",
                    _time: 2021-07-06T23:06:50Z,
                    _value: 2,
                },
            ],
        )
            |> group(columns: ["t0"])
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> group(columns: ["t0"])
            |> last()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

// FIXME: want to extend `flux/planner/group_first_last_test.group_all_filter_field_last` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_all_filter_field_last {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v1",
                    "t1": "t1v1",
                    _time: 2021-07-06T23:06:50Z,
                    _value: 4,
                },
            ],
        )
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> filter(fn: (r) => r._field == "f0")
            |> group()
            |> last()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

// FIXME: want to extend `flux/planner/group_first_last_test.group_one_tag_filter_field_last` but can't
// A sort was added to allow the base case to pass in cloud, but it breaks the pushdown here.
// For now, the body of the testcase is included here, in full, minus the sort.
// Ref: https://github.com/influxdata/influxdb/issues/23757
testcase push_down_group_one_tag_filter_field_last {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])

    want =
        array.from(
            rows: [
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v0",
                    "t1": "t1v1",
                    _time: 2021-07-06T23:06:50Z,
                    _value: 1,
                },
                {
                    _measurement: "m0",
                    _field: "f0",
                    "t0": "t0v1",
                    "t1": "t1v1",
                    _time: 2021-07-06T23:06:50Z,
                    _value: 4,
                },
            ],
        )
            |> group(columns: ["t0"])
    got =
        testing.load(tables: input)
            |> range(start: -100y)
            |> filter(fn: (r) => r._field == "f0")
            |> group(columns: ["t0"])
            |> last()
            |> drop(columns: ["_start", "_stop"])

    testing.diff(got, want) |> yield()
}

testcase
push_down_group_two_tag_filter_field_last
extends
"flux/planner/group_first_last_test.group_two_tag_filter_field_last"
{
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}
