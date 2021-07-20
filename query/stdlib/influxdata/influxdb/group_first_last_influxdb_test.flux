package influxdb_test


import "testing/expect"

testcase push_down_group_one_tag_first extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_one_tag_first()
}

testcase push_down_group_all_filter_field_first extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_all_filter_field_first()
}

testcase push_down_group_one_tag_filter_field_first extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_one_tag_filter_field_first()
}

testcase push_down_group_two_tag_filter_field_first extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_two_tag_filter_field_first()
}

// Group + last tests
testcase push_down_group_one_tag_last extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_one_tag_last()
}

testcase push_down_group_all_filter_field_last extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_all_filter_field_last()
}

testcase push_down_group_one_tag_filter_field_last extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_one_tag_filter_field_last()
}

testcase push_down_group_two_tag_filter_field_last extends "flux/planner/group_first_last_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_first_last_test.group_two_tag_filter_field_last()
}
