package influxdb_test


import "testing/expect"

testcase push_down_tagValues extends "flux/influxdata/influxdb/schema/schema_test.tagValues" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    super()
}
testcase push_down_measurementTagValues extends "flux/influxdata/influxdb/schema/schema_test.measurementTagValues" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    super()
}
testcase push_down_tagKeys extends "flux/influxdata/influxdb/schema/schema_test.tagKeys" {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])
    super()
}
testcase push_down_measurementTagKeys extends "flux/influxdata/influxdb/schema/schema_test.measurementTagKeys" {
    expect.planner(rules: ["PushDownReadTagKeysRule": 1])
    super()
}
testcase push_down_fieldKeys extends "flux/influxdata/influxdb/schema/schema_test.fieldKeys" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    super()
}
testcase push_down_measurementFieldKeys extends "flux/influxdata/influxdb/schema/schema_test.measurementFieldKeys" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    super()
}
testcase push_down_measurements extends "flux/influxdata/influxdb/schema/schema_test.measurements" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    super()
}
