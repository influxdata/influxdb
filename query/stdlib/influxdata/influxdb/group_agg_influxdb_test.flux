package influxdb_test


import "testing/expect"

testcase push_down_group_one_tag_count extends "flux/planner/group_agg_test.group_one_tag_count" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_all_filter_field_count extends "flux/planner/group_agg_test.group_all_filter_field_count" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_one_tag_filter_field_count extends "flux/planner/group_agg_test.group_one_tag_filter_field_count"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_two_tag_filter_field_count extends "flux/planner/group_agg_test.group_two_tag_filter_field_count"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_one_tag_sum extends "flux/planner/group_agg_test.group_one_tag_sum" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_all_filter_field_sum extends "flux/planner/group_agg_test.group_all_filter_field_sum" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_one_tag_filter_field_sum extends "flux/planner/group_agg_test.group_one_tag_filter_field_sum" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_two_tag_filter_field_sum extends "flux/planner/group_agg_test.group_two_tag_filter_field_sum" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}
