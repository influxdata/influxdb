package influxdb_test


import "testing/expect"

testcase push_down_show_fields_with_meas_pred extends "flux/influxdata/influxdb/schema/show_fields_with_meas_pred_test" {
    expect.planner(rules: ["PushDownReadTagValuesRule": 1])
    show_fields_with_meas_pred_test.show_fields_with_meas_pred()
}
