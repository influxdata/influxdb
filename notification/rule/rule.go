package rule

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/flux"
)

var typeToRule = map[string](func() influxdb.NotificationRule){
	"slack":     func() influxdb.NotificationRule { return &Slack{} },
	"pagerduty": func() influxdb.NotificationRule { return &PagerDuty{} },
	"http":      func() influxdb.NotificationRule { return &HTTP{} },
	"telegram":  func() influxdb.NotificationRule { return &Telegram{} },
}

// UnmarshalJSON will convert
func UnmarshalJSON(b []byte) (influxdb.NotificationRule, error) {
	var raw struct {
		Typ string `json:"type"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, &errors.Error{
			Msg: "unable to detect the notification type from json",
		}
	}
	convertedFunc, ok := typeToRule[raw.Typ]
	if !ok {
		return nil, &errors.Error{
			Msg: fmt.Sprintf("invalid notification type %s", raw.Typ),
		}
	}
	converted := convertedFunc()
	err := json.Unmarshal(b, converted)
	return converted, err
}

// Base is the embed struct of every notification rule.
type Base struct {
	ID          platform.ID `json:"id,omitempty"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	EndpointID  platform.ID `json:"endpointID,omitempty"`
	OrgID       platform.ID `json:"orgID,omitempty"`
	OwnerID     platform.ID `json:"ownerID,omitempty"`
	TaskID      platform.ID `json:"taskID,omitempty"`
	// SleepUntil is an optional sleeptime to start a task.
	SleepUntil *time.Time             `json:"sleepUntil,omitempty"`
	Every      *notification.Duration `json:"every,omitempty"`
	// Offset represents a delay before execution.
	// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
	Offset      *notification.Duration    `json:"offset,omitempty"`
	RunbookLink string                    `json:"runbookLink"`
	TagRules    []notification.TagRule    `json:"tagRules,omitempty"`
	StatusRules []notification.StatusRule `json:"statusRules,omitempty"`
	*influxdb.Limit
	influxdb.CRUDLog
}

func (b Base) valid() error {
	if !b.ID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule ID is invalid",
		}
	}
	if b.Name == "" {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule Name can't be empty",
		}
	}
	if !b.OwnerID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule OwnerID is invalid",
		}
	}
	if !b.OrgID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule OrgID is invalid",
		}
	}
	if !b.EndpointID.Valid() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Notification Rule EndpointID is invalid",
		}
	}
	if b.Offset != nil && b.Every != nil && b.Offset.TimeDuration() >= b.Every.TimeDuration() {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Offset should not be equal or greater than the interval",
		}
	}
	for _, tagRule := range b.TagRules {
		if err := tagRule.Valid(); err != nil {
			return err
		}
	}
	if b.Limit != nil {
		if b.Limit.Every <= 0 || b.Limit.Rate <= 0 {
			return &errors.Error{
				Code: errors.EInvalid,
				Msg:  "if limit is set, limit and limitEvery must be larger than 0",
			}
		}
	}

	return nil
}
func (b *Base) generateFluxASTNotificationDefinition(e influxdb.NotificationEndpoint) ast.Statement {
	ruleID := flux.Property("_notification_rule_id", flux.String(b.ID.String()))
	ruleName := flux.Property("_notification_rule_name", flux.String(b.Name))
	endpointID := flux.Property("_notification_endpoint_id", flux.String(b.EndpointID.String()))
	endpointName := flux.Property("_notification_endpoint_name", flux.String(e.GetName()))

	return flux.DefineVariable("notification", flux.Object(ruleID, ruleName, endpointID, endpointName))
}

func (b *Base) generateLevelChecks() []ast.Statement {
	stmts := []ast.Statement{}
	tables := []ast.Expression{}
	for _, r := range b.StatusRules {
		stmt, table := b.generateLevelCheck(r)
		tables = append(tables, table)
		stmts = append(stmts, stmt)
	}

	now := flux.Call(flux.Identifier("now"), flux.Object())
	timeFilter := flux.Function(
		flux.FunctionParams("r"),
		&ast.BinaryExpression{
			Operator: ast.GreaterThanEqualOperator,
			Left:     flux.Member("r", "_time"),
			Right: flux.Call(
				flux.Member("experimental", "subDuration"),
				flux.Object(
					flux.Property("from", now),
					flux.Property("d", (*ast.DurationLiteral)(b.Every)),
				),
			),
		},
	)

	var pipe *ast.PipeExpression
	if len(tables) == 1 {
		pipe = flux.Pipe(
			tables[0],
			flux.Call(
				flux.Identifier("filter"),
				flux.Object(
					flux.Property("fn", timeFilter),
				),
			),
		)
	} else {
		pipe = flux.Pipe(
			flux.Call(
				flux.Identifier("union"),
				flux.Object(
					flux.Property("tables", flux.Array(tables...)),
				),
			),
			flux.Call(
				flux.Identifier("sort"),
				flux.Object(
					flux.Property("columns", flux.Array(flux.String("_time"))),
				),
			),
			flux.Call(
				flux.Identifier("filter"),
				flux.Object(
					flux.Property("fn", timeFilter),
				),
			),
		)
	}

	stmts = append(stmts, flux.DefineVariable("all_statuses", pipe))

	return stmts
}

func (b *Base) generateLevelCheck(r notification.StatusRule) (ast.Statement, *ast.Identifier) {
	var name string
	var pipe *ast.PipeExpression
	if r.PreviousLevel == nil && r.CurrentLevel == notification.Any {
		pipe = flux.Pipe(
			flux.Identifier("statuses"),
			flux.Call(
				flux.Identifier("filter"),
				flux.Object(
					flux.Property("fn", flux.Function(
						flux.FunctionParams("r"),
						flux.Bool(true),
					),
					),
				),
			),
		)
		name = strings.ToLower(r.CurrentLevel.String())
	} else if r.PreviousLevel == nil {
		pipe = flux.Pipe(
			flux.Identifier("statuses"),
			flux.Call(
				flux.Identifier("filter"),
				flux.Object(
					flux.Property("fn", flux.Function(
						flux.FunctionParams("r"),
						flux.Equal(
							flux.Member("r", "_level"),
							flux.String(strings.ToLower(r.CurrentLevel.String())),
						),
					),
					),
				),
			),
		)
		name = strings.ToLower(r.CurrentLevel.String())
	} else {
		fromLevel := strings.ToLower(r.PreviousLevel.String())
		toLevel := strings.ToLower(r.CurrentLevel.String())

		pipe = flux.Pipe(
			flux.Identifier("statuses"),
			flux.Call(
				flux.Member("monitor", "stateChanges"),
				flux.Object(
					flux.Property("fromLevel", flux.String(fromLevel)),
					flux.Property("toLevel", flux.String(toLevel)),
				),
			),
		)
		name = fmt.Sprintf("%s_to_%s", fromLevel, toLevel)
	}

	return flux.DefineVariable(name, pipe), flux.Identifier(name)
}

// increaseDur increases the duration of leading duration in a duration literal.
// It is used so that we will have overlapping windows. If the unit of the literal
// is `s`, we double the interval; otherwise we increase the value by 1. The reason
// for this is to that we query the minimal amount of time that is likely to have data
// in the time range.
//
// This is currently a hack around https://github.com/influxdata/flux/issues/1877
func increaseDur(d *ast.DurationLiteral) *ast.DurationLiteral {
	dur := &ast.DurationLiteral{}
	for i, v := range d.Values {
		value := v
		if i == 0 {
			switch v.Unit {
			case "s", "ms", "us", "ns":
				value.Magnitude *= 2
			default:
				value.Magnitude += 1
			}
		}
		dur.Values = append(dur.Values, value)
	}

	return dur
}

func (b *Base) generateTaskOption() ast.Statement {
	props := []*ast.Property{}

	props = append(props, flux.Property("name", flux.String(b.Name)))

	if b.Every != nil {
		// Make the windows overlap and filter records from previous queries.
		// This is so that we wont miss the first points possible state change.
		props = append(props, flux.Property("every", (*ast.DurationLiteral)(b.Every)))
	}

	if b.Offset != nil {
		props = append(props, flux.Property("offset", (*ast.DurationLiteral)(b.Offset)))
	}

	return flux.DefineTaskOption(flux.Object(props...))
}

func (b *Base) generateFluxASTStatuses() ast.Statement {
	props := []*ast.Property{}

	dur := (*ast.DurationLiteral)(b.Every)
	props = append(props, flux.Property("start", flux.Negative(increaseDur(dur))))

	if len(b.TagRules) > 0 {
		r := b.TagRules[0]
		var body ast.Expression = r.GenerateFluxAST()
		for _, r := range b.TagRules[1:] {
			body = flux.And(body, r.GenerateFluxAST())
		}
		props = append(props, flux.Property("fn", flux.Function(flux.FunctionParams("r"), body)))
	}

	base := flux.Call(flux.Member("monitor", "from"), flux.Object(props...))

	return flux.DefineVariable("statuses", base)
}

// GetID implements influxdb.Getter interface.
func (b Base) GetID() platform.ID {
	return b.ID
}

// GetEndpointID gets the endpointID for a base.
func (b Base) GetEndpointID() platform.ID {
	return b.EndpointID
}

// GetOrgID implements influxdb.Getter interface.
func (b Base) GetOrgID() platform.ID {
	return b.OrgID
}

// GetTaskID gets the task ID for a base.
func (b Base) GetTaskID() platform.ID {
	return b.TaskID
}

// SetTaskID sets the task ID for a base.
func (b *Base) SetTaskID(id platform.ID) {
	b.TaskID = id
}

// ClearPrivateData clears the task ID from the base.
func (b *Base) ClearPrivateData() {
	b.TaskID = 0
}

// MatchesTags returns true if the Rule matches all of the tags
func (b *Base) MatchesTags(tags []influxdb.Tag) bool {
	if len(tags) == 0 {
		return true
	}
	// for each tag in NR
	// if there exists
	// a key value match with operator == equal in tags
	// or
	// a key match with a value mismatch with operator == notequal in tags
	// then true

	for _, NRtag := range b.TagRules {
		isNRTagInFilterTags := false

		for _, filterTag := range tags {
			if NRtag.Key == filterTag.Key {
				if NRtag.Operator == influxdb.Equal && NRtag.Value == filterTag.Value {
					isNRTagInFilterTags = true
				}
				if NRtag.Operator == influxdb.NotEqual && NRtag.Value != filterTag.Value {
					isNRTagInFilterTags = true
				}
			}
		}
		if !isNRTagInFilterTags {
			return false
		}
	}
	return true
}

// GetOwnerID returns the owner id.
func (b Base) GetOwnerID() platform.ID {
	return b.OwnerID
}

// GetCRUDLog implements influxdb.Getter interface.
func (b Base) GetCRUDLog() influxdb.CRUDLog {
	return b.CRUDLog
}

// GetLimit returns the limit pointer.
func (b *Base) GetLimit() *influxdb.Limit {
	return b.Limit
}

// GetName implements influxdb.Getter interface.
func (b *Base) GetName() string {
	return b.Name
}

// GetDescription implements influxdb.Getter interface.
func (b *Base) GetDescription() string {
	return b.Description
}

// SetID will set the primary key.
func (b *Base) SetID(id platform.ID) {
	b.ID = id
}

// SetOrgID will set the org key.
func (b *Base) SetOrgID(id platform.ID) {
	b.OrgID = id
}

// SetOwnerID will set the owner id.
func (b *Base) SetOwnerID(id platform.ID) {
	b.OwnerID = id
}

// SetName implements influxdb.Updator interface.
func (b *Base) SetName(name string) {
	b.Name = name
}

// SetDescription implements influxdb.Updator interface.
func (b *Base) SetDescription(description string) {
	b.Description = description
}
