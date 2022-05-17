package influxdb_test


import "testing/expect"

testcase push_down_group_one_tag_first extends "flux/planner/group_first_last_test.group_one_tag_first" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_all_filter_field_first extends "flux/planner/group_first_last_test.group_all_filter_field_first"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_one_tag_filter_field_first extends "flux/planner/group_first_last_test.group_one_tag_filter_field_first"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_two_tag_filter_field_first extends "flux/planner/group_first_last_test.group_two_tag_filter_field_first"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

// Group + last tests
testcase push_down_group_one_tag_last extends "flux/planner/group_first_last_test.group_one_tag_last" {
    expect.planner(rules: ["PushDownGroupAggregateRule": 1])
    super()
}

testcase push_down_group_all_filter_field_last extends "flux/planner/group_first_last_test.group_all_filter_field_last"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_one_tag_filter_field_last extends "flux/planner/group_first_last_test.group_one_tag_filter_field_last"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }

testcase push_down_group_two_tag_filter_field_last extends "flux/planner/group_first_last_test.group_two_tag_filter_field_last"
{
        expect.planner(rules: ["PushDownGroupAggregateRule": 1])
        super()
    }
