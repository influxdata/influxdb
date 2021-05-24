package influxdb_test

import "testing/expect"

option now = () => (2030-01-01T00:00:00Z)

testcase push_down_min_bare extends "flux/planner/group_min_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_min_test.group_min_bare()
}

testcase push_down_min_bare_host extends "flux/planner/group_min_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_min_test.group_min_bare_host()
}

testcase push_down_min_bare_field extends "flux/planner/group_min_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_min_test.group_min_bare_field()
}

testcase push_down_max_bare extends "flux/planner/group_max_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_max_test.group_max_bare()
}

testcase push_down_max_bare_host extends "flux/planner/group_max_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_max_test.group_max_bare_host()
}

testcase push_down_max_bare_field extends "flux/planner/group_max_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_max_test.group_max_bare_field()
}

testcase push_down_table_test_min extends "flux/planner/group_min_max_table_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_min_max_table_test.group_min_table()
}

testcase push_down_table_test_max extends "flux/planner/group_min_max_table_test" {
    expect.planner(rules: [
        "PushDownGroupAggregateRule": 1,
    ])
    group_min_max_table_test.group_max_table()
}
