package tests

import (
	"net/http"
	"time"

	"github.com/influxdata/influxdb/v2"
	influxhttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notification"
	checks "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

// ValidCustomNotificationEndpoint creates a NotificationEndpoint with a custom name
func ValidCustomNotificationEndpoint(org platform.ID, name string) influxdb.NotificationEndpoint {
	return &endpoint.HTTP{
		Base: endpoint.Base{
			Name:    name,
			OrgID:   &org,
			Status:  influxdb.Active,
			CRUDLog: influxdb.CRUDLog{},
		},
		URL:        "https://howdy.com",
		AuthMethod: "none",
		Method:     http.MethodGet,
	}
}

// ValidNotificationEndpoint returns a valid notification endpoint.
// This is the easiest way of "mocking" a influxdb.NotificationEndpoint.
func ValidNotificationEndpoint(org platform.ID) influxdb.NotificationEndpoint {
	return ValidCustomNotificationEndpoint(org, "howdy")
}

// ValidNotificationRule returns a valid Notification Rule of type HTTP for testing
func ValidNotificationRule(org, endpoint platform.ID) influxdb.NotificationRule {
	d, _ := notification.FromTimeDuration(time.Second * 5)
	return &rule.HTTP{
		Base: rule.Base{
			Name:       "little rule",
			EndpointID: endpoint,
			OrgID:      org,
			Every:      &d,
			CRUDLog:    influxdb.CRUDLog{},
			StatusRules: []notification.StatusRule{
				{
					CurrentLevel: notification.Critical,
				},
			},
			TagRules: []notification.TagRule{},
		},
	}
}

// MockCheck returns a valid check to be used in tests.
func MockCheck(name string, orgID, userID platform.ID) *influxhttp.Check {
	return &influxhttp.Check{
		ID:                    userID,
		OwnerID:               userID,
		Type:                  "threshold",
		Status:                influxdb.Active,
		Name:                  name,
		Description:           "pipeline test check",
		OrgID:                 orgID,
		Every:                 "1m",
		Offset:                "0m",
		Level:                 "CRIT",
		StatusMessageTemplate: "Check: ${ r._check_name } is: ${ r._level }",
		Query: &influxhttp.CheckQuery{
			Name:     name,
			Text:     `from(bucket: "db/rp") |> range(start: v.timeRangeStart, stop: v.timeRangeStop) |> filter(fn: (r) => r._measurement == "my_measurement") |> filter(fn: (r) => r._field == "my_field") |> count() |> yield(name: "count")`,
			EditMode: "builder",
			BuilderConfig: &influxhttp.CheckBuilderConfig{
				Buckets: []string{"db/rp"},
				Tags: []struct {
					Key                   string   `json:"key"`
					Values                []string `json:"values"`
					AggregateFunctionType string   `json:"aggregateFunctionType"`
				}{
					{
						Key:                   "_measurement",
						Values:                []string{"my_measurement"},
						AggregateFunctionType: "filter",
					},
					{
						Key:                   "_field",
						Values:                []string{"my_field"},
						AggregateFunctionType: "filter",
					},
				},
				Functions: []struct {
					Name string `json:"name"`
				}{
					{
						Name: "count",
					},
				},
				AggregateWindow: struct {
					Period string `json:"period"`
				}{
					Period: "1m",
				},
			},
		},
		Thresholds: []*influxhttp.CheckThreshold{
			{
				Type:  "greater",
				Value: 9999,
				ThresholdConfigBase: checks.ThresholdConfigBase{
					Level: notification.Critical,
				},
			},
		},
	}
}
