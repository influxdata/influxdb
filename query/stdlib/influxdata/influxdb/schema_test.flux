package influxdb_test


import "testing/expect"
import "internal/debug"
import "array"
import "testing"
import "csv"
import "influxdata/influxdb/schema"

// TODO(nathanielc): The test data below is duplicated from the Flux test code.
// This is because we can't use options in tests that extend other tests.
// This makes it so that the tests pass and do the same thing, but any changes
// to the source data in Flux will need to be reflected here.
// Range of the input data
start = 2018-05-22T19:53:16Z
stop = 2018-05-22T19:53:26Z

// cpu:
//      tags: host,cpu
//      fields: usage_idle,usage_user
cpu =
    array.from(
        rows: [
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.83,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.72,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.97,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.local",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_idle",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_idle",
                _value: 1.92,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu0",
                _field: "usage_user",
                _value: 1.95,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "cpu",
                host: "host.global",
                cpu: "cpu1",
                _field: "usage_user",
                _value: 1.92,
            },
        ],
    )
        |> group(columns: ["_measurement", "_field", "host", "cpu"])

// swap:
//      tags: host,partition
//      fields: used_percent
swap =
    array.from(
        rows: [
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "swap",
                host: "host.local",
                partition: "/dev/sda1",
                _field: "used_percent",
                _value: 66.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "swap",
                host: "host.local",
                partition: "/dev/sdb1",
                _field: "used_percent",
                _value: 66.98,
            },
            {
                _time: 2018-05-22T19:53:26Z,
                _measurement: "swap",
                host: "host.local",
                partition: "/dev/sda1",
                _field: "used_percent",
                _value: 37.59,
            },
            {
                _time: 2018-05-22T19:53:26Z,
                _measurement: "swap",
                host: "host.local",
                partition: "/dev/sdb1",
                _field: "used_percent",
                _value: 37.59,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "swap",
                host: "host.global",
                partition: "/dev/sda1",
                _field: "used_percent",
                _value: 72.98,
            },
            {
                _time: 2018-05-22T19:53:16Z,
                _measurement: "swap",
                host: "host.global",
                partition: "/dev/sdb1",
                _field: "used_percent",
                _value: 72.98,
            },
            {
                _time: 2018-05-22T19:53:26Z,
                _measurement: "swap",
                host: "host.global",
                partition: "/dev/sda1",
                _field: "used_percent",
                _value: 88.59,
            },
            {
                _time: 2018-05-22T19:53:26Z,
                _measurement: "swap",
                host: "host.global",
                partition: "/dev/sdb1",
                _field: "used_percent",
                _value: 88.59,
            },
        ],
    )
        |> group(columns: ["_measurement", "_field", "host", "partition"])

option schema._from = (bucket) =>
    union(
        tables: [
            cpu
                |> debug.opaque(),
            swap
                |> debug.opaque(),
        ],
    )
        |> testing.load()

testcase push_down_tagValues extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    schema_test.tagValues()
}
testcase push_down_measurementTagValues extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    schema_test.measurementTagValues()
}
testcase push_down_tagKeys extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])
    schema_test.tagKeys()
}
testcase push_down_measurementTagKeys extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])
    schema_test.measurementTagKeys()
}
testcase push_down_fieldKeys extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    schema_test.fieldKeys()
}
testcase push_down_measurementFieldKeys extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    schema_test.measurementFieldKeys()
}
testcase push_down_measurements extends "flux/influxdata/influxdb/schema/schema_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    schema_test.measurements()
}
