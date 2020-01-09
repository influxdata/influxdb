package endpoints

// types of endpoints.
const (
	TypeHTTP      = "http"
	TypePagerDuty = "pagerduty"
	TypeSlack     = "slack"
)

var availableTypes = []string{TypeHTTP, TypePagerDuty, TypeSlack}
