package http

import (
    "common"
)

type SubscriptionManager interface {
    // Ensure this is a correct description
    // This command outputs all subscriptions associated with the given database
//    ListSubscriptions(requester common.User) ([]int, error)
    ListSubscriptions(requester common.User) error
    // This command adds a current TimeSeries subscription for the specified
    // TimeSeries Ids to the subscription list. Each execution of Query-Sub will
    // output the current TimeSeries value for the specified Ids if that value
    // has been updated since the last execution of Query-Sub
//    SubscribeCurrent(requester common.User) error
    // This command adds a TimeSeries subscription fot the specified TimeSeries
    // Ids to the subscription list, beginning at START_TM and optionally ending at
    // END_TM. Each execution of Query-Sub will output all new TimeSeries values
    // for the specified Ids if that value has been updated since the last
    // execution of Query-Sub
    SubscribeTimeSeries(requester common.User) error
    // Samples are selected whose timestamps are greater than or equal to START_TM
    // and are strictly less than END_TM (i.e. END_TM is non-inclusive)
    // If END_TM is not specified, "now" is assumed (i.e. fetch all data since 
    // START_TM)
    // .. more available..
//    QueryCurrent(requester common.User) error
    // This command behaves equivalently to Query-Timeseries above, except that
    // only the most current (last) sample whose timestamp is less than END_TM
    // (if specified) is output
//    QueryFollow(requester common.User) error
    // This command revokes a previous "current" or "timeseries" subscription
//    Unsubscribe(requester common.User, ids []int) error
    // This command will output data for all TimeSeries Ids in the subscription
    // list, if there is new data for those TimeSeries Ids since the last
    // execution of Query-Sub
//    QuerySub(requester common.User) error
}
