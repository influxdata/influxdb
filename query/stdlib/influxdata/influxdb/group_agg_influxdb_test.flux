package influxdb_test

import "testing/expect"

testcase push_down_group_one_tag_count extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_one_tag_count()
}

testcase push_down_group_all_filter_field_count extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_all_filter_field_count()
}

testcase push_down_group_one_tag_filter_field_count extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_one_tag_filter_field_count()
}

testcase push_down_group_two_tag_filter_field_count extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_two_tag_filter_field_count()
}

testcase push_down_group_one_tag_sum extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_one_tag_sum()
}

testcase push_down_group_all_filter_field_sum extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_all_filter_field_sum()
}

testcase push_down_group_one_tag_filter_field_sum extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_one_tag_filter_field_sum()
}

testcase push_down_group_two_tag_filter_field_sum extends "flux/planner/group_agg_test" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    group_agg_test.group_two_tag_filter_field_sum()
}
