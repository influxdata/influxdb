package influxdb_test


import "testing/expect"

testcase push_down_min_bare extends "flux/planner/group_min_test.group_min_bare" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_min_bare_host extends "flux/planner/group_min_test.group_min_bare_host" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_min_bare_field extends "flux/planner/group_min_test.group_min_bare_field" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_max_bare extends "flux/planner/group_max_test.group_max_bare" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_max_bare_host extends "flux/planner/group_max_test.group_max_bare_host" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_max_bare_field extends "flux/planner/group_max_test.group_max_bare_field" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_table_test_min extends "flux/planner/group_min_max_table_test.group_min_table" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_table_test_max extends "flux/planner/group_min_max_table_test.group_max_table" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}
