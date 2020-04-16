package pkger

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

// Package kind types.
const (
	KindUnknown                       Kind = ""
	KindBucket                        Kind = "Bucket"
	KindCheck                         Kind = "Check"
	KindCheckDeadman                  Kind = "CheckDeadman"
	KindCheckThreshold                Kind = "CheckThreshold"
	KindDashboard                     Kind = "Dashboard"
	KindLabel                         Kind = "Label"
	KindNotificationEndpoint          Kind = "NotificationEndpoint"
	KindNotificationEndpointHTTP      Kind = "NotificationEndpointHTTP"
	KindNotificationEndpointPagerDuty Kind = "NotificationEndpointPagerDuty"
	KindNotificationEndpointSlack     Kind = "NotificationEndpointSlack"
	KindNotificationRule              Kind = "NotificationRule"
	KindPackage                       Kind = "Package"
	KindTask                          Kind = "Task"
	KindTelegraf                      Kind = "Telegraf"
	KindVariable                      Kind = "Variable"
)

var kinds = map[Kind]bool{
	KindBucket:                        true,
	KindCheck:                         true,
	KindCheckDeadman:                  true,
	KindCheckThreshold:                true,
	KindDashboard:                     true,
	KindLabel:                         true,
	KindNotificationEndpoint:          true,
	KindNotificationEndpointHTTP:      true,
	KindNotificationEndpointPagerDuty: true,
	KindNotificationEndpointSlack:     true,
	KindNotificationRule:              true,
	KindTask:                          true,
	KindTelegraf:                      true,
	KindVariable:                      true,
}

// Kind is a resource kind.
type Kind string

// String provides the kind in human readable form.
func (k Kind) String() string {
	if kinds[k] {
		return string(k)
	}
	if k == KindUnknown {
		return "unknown"
	}
	return string(k)
}

// OK validates the kind is valid.
func (k Kind) OK() error {
	if k == KindUnknown {
		return errors.New("invalid kind")
	}
	if !kinds[k] {
		return errors.New("unsupported kind provided")
	}
	return nil
}

// ResourceType converts a kind to a known resource type (if applicable).
func (k Kind) ResourceType() influxdb.ResourceType {
	switch k {
	case KindBucket:
		return influxdb.BucketsResourceType
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		return influxdb.ChecksResourceType
	case KindDashboard:
		return influxdb.DashboardsResourceType
	case KindLabel:
		return influxdb.LabelsResourceType
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		return influxdb.NotificationEndpointResourceType
	case KindNotificationRule:
		return influxdb.NotificationRuleResourceType
	case KindTask:
		return influxdb.TasksResourceType
	case KindTelegraf:
		return influxdb.TelegrafsResourceType
	case KindVariable:
		return influxdb.VariablesResourceType
	default:
		return ""
	}
}

func (k Kind) is(comps ...Kind) bool {
	for _, c := range comps {
		if c == k {
			return true
		}
	}
	return false
}

// SafeID is an equivalent influxdb.ID that encodes safely with
// zero values (influxdb.ID == 0).
type SafeID influxdb.ID

// Encode will safely encode the id.
func (s SafeID) Encode() ([]byte, error) {
	id := influxdb.ID(s)
	b, _ := id.Encode()
	return b, nil
}

// String prints a encoded string representation of the id.
func (s SafeID) String() string {
	return influxdb.ID(s).String()
}

// DiffIdentifier are the identifying fields for any given resource. Each resource
// dictates if the resource is new, to be removed, or will remain.
type DiffIdentifier struct {
	ID          SafeID      `json:"id"`
	Remove      bool        `json:"bool"`
	StateStatus StateStatus `json:"stateStatus"`
	PkgName     string      `json:"pkgName"`
}

// IsNew indicates the resource is new to the platform.
func (d DiffIdentifier) IsNew() bool {
	return d.ID == 0
}

// Diff is the result of a service DryRun call. The diff outlines
// what is new and or updated from the current state of the platform.
type Diff struct {
	Buckets               []DiffBucket               `json:"buckets"`
	Checks                []DiffCheck                `json:"checks"`
	Dashboards            []DiffDashboard            `json:"dashboards"`
	Labels                []DiffLabel                `json:"labels"`
	LabelMappings         []DiffLabelMapping         `json:"labelMappings"`
	NotificationEndpoints []DiffNotificationEndpoint `json:"notificationEndpoints"`
	NotificationRules     []DiffNotificationRule     `json:"notificationRules"`
	Tasks                 []DiffTask                 `json:"tasks"`
	Telegrafs             []DiffTelegraf             `json:"telegrafConfigs"`
	Variables             []DiffVariable             `json:"variables"`
}

// HasConflicts provides a binary t/f if there are any changes within package
// after dry run is complete.
func (d Diff) HasConflicts() bool {
	for _, b := range d.Buckets {
		if b.hasConflict() {
			return true
		}
	}

	for _, l := range d.Labels {
		if l.hasConflict() {
			return true
		}
	}

	for _, v := range d.Variables {
		if v.hasConflict() {
			return true
		}
	}

	return false
}

type (
	// DiffBucket is a diff of an individual bucket.
	DiffBucket struct {
		DiffIdentifier

		New DiffBucketValues  `json:"new"`
		Old *DiffBucketValues `json:"old"`
	}

	// DiffBucketValues are the varying values for a bucket.
	DiffBucketValues struct {
		Name           string         `json:"name"`
		Description    string         `json:"description"`
		RetentionRules retentionRules `json:"retentionRules"`
	}
)

func (d DiffBucket) hasConflict() bool {
	return !d.IsNew() && d.Old != nil && !reflect.DeepEqual(*d.Old, d.New)
}

// DiffCheckValues are the varying values for a check.
type DiffCheckValues struct {
	influxdb.Check
}

// MarshalJSON implementation here is forced by the embedded check value here.
func (d DiffCheckValues) MarshalJSON() ([]byte, error) {
	if d.Check == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(d.Check)
}

// UnmarshalJSON decodes the check values.
func (d *DiffCheckValues) UnmarshalJSON(b []byte) (err error) {
	d.Check, err = icheck.UnmarshalJSON(b)
	if influxdb.EInternal == influxdb.ErrorCode(err) {
		return nil
	}
	return err
}

// DiffCheck is a diff of an individual check.
type DiffCheck struct {
	DiffIdentifier

	New DiffCheckValues  `json:"new"`
	Old *DiffCheckValues `json:"old"`
}

type (
	// DiffDashboard is a diff of an individual dashboard.
	DiffDashboard struct {
		DiffIdentifier

		New DiffDashboardValues  `json:"new"`
		Old *DiffDashboardValues `json:"old"`
	}

	// DiffDashboardValues are values for a dashboard.
	DiffDashboardValues struct {
		Name   string      `json:"name"`
		Desc   string      `json:"description"`
		Charts []DiffChart `json:"charts"`
	}
)

// DiffChart is a diff of oa chart. Since all charts are new right now.
// the SummaryChart is reused here.
type DiffChart SummaryChart

type (
	// DiffLabel is a diff of an individual label.
	DiffLabel struct {
		DiffIdentifier

		New DiffLabelValues  `json:"new"`
		Old *DiffLabelValues `json:"old"`
	}

	// DiffLabelValues are the varying values for a label.
	DiffLabelValues struct {
		Name        string `json:"name"`
		Color       string `json:"color"`
		Description string `json:"description"`
	}
)

func (d DiffLabel) hasConflict() bool {
	return !d.IsNew() && d.Old != nil && *d.Old != d.New
}

// StateStatus indicates the status of a diff or summary resource
type StateStatus string

const (
	StateStatusExists StateStatus = "exists"
	StateStatusNew    StateStatus = "new"
	StateStatusRemove StateStatus = "remove"
)

// DiffLabelMapping is a diff of an individual label mapping. A
// single resource may have multiple mappings to multiple labels.
// A label can have many mappings to other resources.
type DiffLabelMapping struct {
	StateStatus StateStatus `json:"stateStatus"`

	ResType    influxdb.ResourceType `json:"resourceType"`
	ResID      SafeID                `json:"resourceID"`
	ResName    string                `json:"resourceName"`
	ResPkgName string                `json:"resourcePkgName"`

	LabelID      SafeID `json:"labelID"`
	LabelName    string `json:"labelName"`
	LabelPkgName string `json:"labelPkgName"`
}

//func (d DiffLabelMapping) IsNew() bool {
//	return d.StateStatus == StateStatusNew
//}

// DiffNotificationEndpointValues are the varying values for a notification endpoint.
type DiffNotificationEndpointValues struct {
	influxdb.NotificationEndpoint
}

// MarshalJSON implementation here is forced by the embedded check value here.
func (d DiffNotificationEndpointValues) MarshalJSON() ([]byte, error) {
	if d.NotificationEndpoint == nil {
		return json.Marshal(nil)
	}
	return json.Marshal(d.NotificationEndpoint)
}

// UnmarshalJSON decodes the notification endpoint. This is necessary unfortunately.
func (d *DiffNotificationEndpointValues) UnmarshalJSON(b []byte) (err error) {
	d.NotificationEndpoint, err = endpoint.UnmarshalJSON(b)
	if influxdb.EInvalid == influxdb.ErrorCode(err) {
		return nil
	}
	return
}

// DiffNotificationEndpoint is a diff of an individual notification endpoint.
type DiffNotificationEndpoint struct {
	DiffIdentifier

	New DiffNotificationEndpointValues  `json:"new"`
	Old *DiffNotificationEndpointValues `json:"old"`
}

type (
	// DiffNotificationRule is a diff of an individual notification rule.
	DiffNotificationRule struct {
		DiffIdentifier

		New DiffNotificationRuleValues  `json:"new"`
		Old *DiffNotificationRuleValues `json:"old"`
	}

	// DiffNotificationRuleValues are the values for an individual rule.
	DiffNotificationRuleValues struct {
		Name        string `json:"name"`
		Description string `json:"description"`

		// These 3 fields represent the relationship of the rule to the endpoint.
		EndpointID   SafeID `json:"endpointID"`
		EndpointName string `json:"endpointName"`
		EndpointType string `json:"endpointType"`

		Every           string              `json:"every"`
		Offset          string              `json:"offset"`
		MessageTemplate string              `json:"messageTemplate"`
		StatusRules     []SummaryStatusRule `json:"statusRules"`
		TagRules        []SummaryTagRule    `json:"tagRules"`
	}
)

func newDiffNotificationRule(r *notificationRule, iEndpoint influxdb.NotificationEndpoint) DiffNotificationRule {
	sum := DiffNotificationRule{
		DiffIdentifier: DiffIdentifier{
			ID:      SafeID(r.ID()),
			Remove:  r.shouldRemove,
			PkgName: r.PkgName(),
		},
		New: DiffNotificationRuleValues{
			Name:            r.Name(),
			Description:     r.description,
			EndpointName:    r.endpointName.String(),
			Every:           r.every.String(),
			Offset:          r.offset.String(),
			MessageTemplate: r.msgTemplate,
			StatusRules:     toSummaryStatusRules(r.statusRules),
			TagRules:        toSummaryTagRules(r.tagRules),
		},
	}
	if iEndpoint != nil {
		sum.New.EndpointID = SafeID(iEndpoint.GetID())
		sum.New.EndpointType = iEndpoint.Type()
	}

	if r.existing == nil {
		return sum
	}

	sum.Old = &DiffNotificationRuleValues{
		Name:         r.existing.rule.GetName(),
		Description:  r.existing.rule.GetDescription(),
		EndpointName: r.existing.endpointName,
		EndpointID:   SafeID(r.existing.rule.GetEndpointID()),
		EndpointType: r.existing.endpointType,
	}

	assignBase := func(b rule.Base) {
		if b.Every != nil {
			sum.Old.Every = b.Every.TimeDuration().String()
		}
		if b.Offset != nil {
			sum.Old.Offset = b.Offset.TimeDuration().String()
		}
		for _, tr := range b.TagRules {
			sum.Old.TagRules = append(sum.Old.TagRules, SummaryTagRule{
				Key:      tr.Key,
				Value:    tr.Value,
				Operator: tr.Operator.String(),
			})
		}
		for _, sr := range b.StatusRules {
			sRule := SummaryStatusRule{CurrentLevel: sr.CurrentLevel.String()}
			if sr.PreviousLevel != nil {
				sRule.PreviousLevel = sr.PreviousLevel.String()
			}
			sum.Old.StatusRules = append(sum.Old.StatusRules, sRule)
		}
	}

	switch p := r.existing.rule.(type) {
	case *rule.HTTP:
		assignBase(p.Base)
	case *rule.Slack:
		assignBase(p.Base)
		sum.Old.MessageTemplate = p.MessageTemplate
	case *rule.PagerDuty:
		assignBase(p.Base)
		sum.Old.MessageTemplate = p.MessageTemplate
	}

	return sum
}

type (
	// DiffTask is a diff of an individual task.
	DiffTask struct {
		DiffIdentifier

		New DiffTaskValues  `json:"new"`
		Old *DiffTaskValues `json:"old"`
	}

	// DiffTaskValues are the values for an individual task.
	DiffTaskValues struct {
		Name        string          `json:"name"`
		Cron        string          `json:"cron"`
		Description string          `json:"description"`
		Every       string          `json:"every"`
		Offset      string          `json:"offset"`
		Query       string          `json:"query"`
		Status      influxdb.Status `json:"status"`
	}
)

// DiffTelegraf is a diff of an individual telegraf. This resource is always new.
type DiffTelegraf struct {
	DiffIdentifier

	New influxdb.TelegrafConfig  `json:"new"`
	Old *influxdb.TelegrafConfig `json:"old"`
}

type (
	// DiffVariable is a diff of an individual variable.
	DiffVariable struct {
		DiffIdentifier

		New DiffVariableValues  `json:"new"`
		Old *DiffVariableValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
	}

	// DiffVariableValues are the varying values for a variable.
	DiffVariableValues struct {
		Name        string                      `json:"name"`
		Description string                      `json:"description"`
		Args        *influxdb.VariableArguments `json:"args"`
	}
)

func (d DiffVariable) hasConflict() bool {
	return !d.IsNew() && d.Old != nil && !reflect.DeepEqual(*d.Old, d.New)
}

// Summary is a definition of all the resources that have or
// will be created from a pkg.
type Summary struct {
	Buckets               []SummaryBucket               `json:"buckets"`
	Checks                []SummaryCheck                `json:"checks"`
	Dashboards            []SummaryDashboard            `json:"dashboards"`
	NotificationEndpoints []SummaryNotificationEndpoint `json:"notificationEndpoints"`
	NotificationRules     []SummaryNotificationRule     `json:"notificationRules"`
	Labels                []SummaryLabel                `json:"labels"`
	LabelMappings         []SummaryLabelMapping         `json:"labelMappings"`
	MissingEnvs           []string                      `json:"missingEnvRefs"`
	MissingSecrets        []string                      `json:"missingSecrets"`
	Tasks                 []SummaryTask                 `json:"summaryTask"`
	TelegrafConfigs       []SummaryTelegraf             `json:"telegrafConfigs"`
	Variables             []SummaryVariable             `json:"variables"`
}

// SummaryBucket provides a summary of a pkg bucket.
type SummaryBucket struct {
	ID          SafeID `json:"id,omitempty"`
	OrgID       SafeID `json:"orgID,omitempty"`
	Name        string `json:"name"`
	PkgName     string `json:"pkgName"`
	Description string `json:"description"`
	// TODO: return retention rules?
	RetentionPeriod   time.Duration  `json:"retentionPeriod"`
	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// SummaryCheck provides a summary of a pkg check.
type SummaryCheck struct {
	PkgName           string          `json:"pkgName"`
	Check             influxdb.Check  `json:"check"`
	Status            influxdb.Status `json:"status"`
	LabelAssociations []SummaryLabel  `json:"labelAssociations"`
}

func (s *SummaryCheck) UnmarshalJSON(b []byte) error {
	var out struct {
		PkgName           string          `json:"pkgName"`
		Status            string          `json:"status"`
		LabelAssociations []SummaryLabel  `json:"labelAssociations"`
		Check             json.RawMessage `json:"check"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return err
	}
	s.PkgName = out.PkgName
	s.Status = influxdb.Status(out.Status)
	s.LabelAssociations = out.LabelAssociations

	var err error
	s.Check, err = icheck.UnmarshalJSON(out.Check)
	return err
}

// SummaryDashboard provides a summary of a pkg dashboard.
type SummaryDashboard struct {
	ID          SafeID         `json:"id"`
	OrgID       SafeID         `json:"orgID"`
	PkgName     string         `json:"pkgName"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Charts      []SummaryChart `json:"charts"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// SummaryChart provides a summary of a pkg dashboard's chart.
type SummaryChart struct {
	Properties influxdb.ViewProperties `json:"-"`

	XPosition int `json:"xPos"`
	YPosition int `json:"yPos"`
	Height    int `json:"height"`
	Width     int `json:"width"`
}

// MarshalJSON marshals a summary chart.
func (s *SummaryChart) MarshalJSON() ([]byte, error) {
	b, err := influxdb.MarshalViewPropertiesJSON(s.Properties)
	if err != nil {
		return nil, err
	}

	type alias SummaryChart

	out := struct {
		Props json.RawMessage `json:"properties"`
		alias
	}{
		Props: b,
		alias: alias(*s),
	}
	return json.Marshal(out)
}

// UnmarshalJSON unmarshals a view properities and other data.
func (s *SummaryChart) UnmarshalJSON(b []byte) error {
	type alias SummaryChart
	a := (*alias)(s)
	if err := json.Unmarshal(b, a); err != nil {
		return err
	}
	s.XPosition = a.XPosition
	s.XPosition = a.YPosition
	s.Height = a.Height
	s.Width = a.Width

	vp, err := influxdb.UnmarshalViewPropertiesJSON(b)
	if err != nil {
		return err
	}
	s.Properties = vp
	return nil
}

// SummaryNotificationEndpoint provides a summary of a pkg notification endpoint.
type SummaryNotificationEndpoint struct {
	PkgName              string                        `json:"pkgName"`
	NotificationEndpoint influxdb.NotificationEndpoint `json:"notificationEndpoint"`
	LabelAssociations    []SummaryLabel                `json:"labelAssociations"`
}

// UnmarshalJSON unmarshals the notificatio endpoint. This is necessary b/c of
// the notification endpoint does not have a means ot unmarshal itself.
func (s *SummaryNotificationEndpoint) UnmarshalJSON(b []byte) error {
	var a struct {
		PkgName              string          `json:"pkgName"`
		NotificationEndpoint json.RawMessage `json:"notificationEndpoint"`
		LabelAssociations    []SummaryLabel  `json:"labelAssociations"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	s.PkgName = a.PkgName
	s.LabelAssociations = a.LabelAssociations

	e, err := endpoint.UnmarshalJSON(a.NotificationEndpoint)
	s.NotificationEndpoint = e
	return err
}

// Summary types for NotificationRules which provide a summary of a pkg notification rule.
type (
	SummaryNotificationRule struct {
		ID          SafeID `json:"id"`
		PkgName     string `json:"pkgName"`
		Name        string `json:"name"`
		Description string `json:"description"`

		// These 3 fields represent the relationship of the rule to the endpoint.
		EndpointID   SafeID `json:"endpointID"`
		EndpointName string `json:"endpointName"`
		EndpointType string `json:"endpointType"`

		Every             string              `json:"every"`
		LabelAssociations []SummaryLabel      `json:"labelAssociations"`
		Offset            string              `json:"offset"`
		MessageTemplate   string              `json:"messageTemplate"`
		Status            influxdb.Status     `json:"status"`
		StatusRules       []SummaryStatusRule `json:"statusRules"`
		TagRules          []SummaryTagRule    `json:"tagRules"`
	}

	SummaryStatusRule struct {
		CurrentLevel  string `json:"currentLevel"`
		PreviousLevel string `json:"previousLevel"`
	}

	SummaryTagRule struct {
		Key      string `json:"key"`
		Value    string `json:"value"`
		Operator string `json:"operator"`
	}
)

// SummaryLabel provides a summary of a pkg label.
type SummaryLabel struct {
	ID         SafeID `json:"id"`
	OrgID      SafeID `json:"orgID"`
	PkgName    string `json:"pkgName"`
	Name       string `json:"name"`
	Properties struct {
		Color       string `json:"color"`
		Description string `json:"description"`
	} `json:"properties"`
}

// SummaryLabelMapping provides a summary of a label mapped with a single resource.
type SummaryLabelMapping struct {
	exists          bool
	Status          StateStatus           `json:"status,omitempty"`
	ResourceID      SafeID                `json:"resourceID"`
	ResourcePkgName string                `json:"resourcePkgName"`
	ResourceName    string                `json:"resourceName"`
	ResourceType    influxdb.ResourceType `json:"resourceType"`
	LabelPkgName    string                `json:"labelPkgName"`
	LabelName       string                `json:"labelName"`
	LabelID         SafeID                `json:"labelID"`
}

// SummaryTask provides a summary of a task.
type SummaryTask struct {
	ID          SafeID          `json:"id"`
	PkgName     string          `json:"pkgName"`
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Description string          `json:"description"`
	Every       string          `json:"every"`
	Offset      string          `json:"offset"`
	Query       string          `json:"query"`
	Status      influxdb.Status `json:"status"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// SummaryTelegraf provides a summary of a pkg telegraf config.
type SummaryTelegraf struct {
	PkgName           string                  `json:"pkgName"`
	TelegrafConfig    influxdb.TelegrafConfig `json:"telegrafConfig"`
	LabelAssociations []SummaryLabel          `json:"labelAssociations"`
}

// SummaryVariable provides a summary of a pkg variable.
type SummaryVariable struct {
	ID                SafeID                      `json:"id,omitempty"`
	PkgName           string                      `json:"pkgName"`
	OrgID             SafeID                      `json:"orgID,omitempty"`
	Name              string                      `json:"name"`
	Description       string                      `json:"description"`
	Arguments         *influxdb.VariableArguments `json:"arguments"`
	LabelAssociations []SummaryLabel              `json:"labelAssociations"`
}

const (
	fieldNotificationRuleChannel         = "channel"
	fieldNotificationRuleCurrentLevel    = "currentLevel"
	fieldNotificationRuleEndpointName    = "endpointName"
	fieldNotificationRuleMessageTemplate = "messageTemplate"
	fieldNotificationRulePreviousLevel   = "previousLevel"
	fieldNotificationRuleStatusRules     = "statusRules"
	fieldNotificationRuleTagRules        = "tagRules"
)

type notificationRule struct {
	identity

	id    influxdb.ID
	orgID influxdb.ID

	channel     string
	description string
	every       time.Duration
	msgTemplate string
	offset      time.Duration
	status      string
	statusRules []struct{ curLvl, prevLvl string }
	tagRules    []struct{ k, v, op string }

	endpointID   influxdb.ID
	endpointName *references
	endpointType string

	existing *existingRule

	labels sortedLabels
}

type existingRule struct {
	rule         influxdb.NotificationRule
	endpointName string
	endpointType string
}

func (r *notificationRule) Exists() bool {
	return r.existing != nil
}

func (r *notificationRule) ID() influxdb.ID {
	if r.existing != nil {
		return r.existing.rule.GetID()
	}
	return r.id
}

func (r *notificationRule) Labels() []*label {
	return r.labels
}

func (r *notificationRule) ResourceType() influxdb.ResourceType {
	return KindNotificationRule.ResourceType()
}

func (r *notificationRule) Status() influxdb.Status {
	if r.status == "" {
		return influxdb.Active
	}
	return influxdb.Status(r.status)
}

func (r *notificationRule) summarize() SummaryNotificationRule {
	return SummaryNotificationRule{
		ID:                SafeID(r.ID()),
		PkgName:           r.PkgName(),
		Name:              r.Name(),
		EndpointID:        SafeID(r.endpointID),
		EndpointName:      r.endpointName.String(),
		EndpointType:      r.endpointType,
		Description:       r.description,
		Every:             r.every.String(),
		LabelAssociations: toSummaryLabels(r.labels...),
		Offset:            r.offset.String(),
		MessageTemplate:   r.msgTemplate,
		Status:            r.Status(),
		StatusRules:       toSummaryStatusRules(r.statusRules),
		TagRules:          toSummaryTagRules(r.tagRules),
	}
}

func (r *notificationRule) toInfluxRule() influxdb.NotificationRule {
	base := rule.Base{
		ID:          r.ID(),
		Name:        r.Name(),
		Description: r.description,
		EndpointID:  r.endpointID,
		OrgID:       r.orgID,
		Every:       toNotificationDuration(r.every),
		Offset:      toNotificationDuration(r.offset),
	}
	for _, sr := range r.statusRules {
		var prevLvl *notification.CheckLevel
		if lvl := notification.ParseCheckLevel(sr.prevLvl); lvl != notification.Unknown {
			prevLvl = &lvl
		}
		base.StatusRules = append(base.StatusRules, notification.StatusRule{
			CurrentLevel:  notification.ParseCheckLevel(sr.curLvl),
			PreviousLevel: prevLvl,
		})
	}
	for _, tr := range r.tagRules {
		op, _ := influxdb.ToOperator(tr.op)
		base.TagRules = append(base.TagRules, notification.TagRule{
			Tag: influxdb.Tag{
				Key:   tr.k,
				Value: tr.v,
			},
			Operator: op,
		})
	}

	switch r.endpointType {
	case "http":
		return &rule.HTTP{Base: base}
	case "pagerduty":
		return &rule.PagerDuty{
			Base:            base,
			MessageTemplate: r.msgTemplate,
		}
	case "slack":
		return &rule.Slack{
			Base:            base,
			Channel:         r.channel,
			MessageTemplate: r.msgTemplate,
		}
	}
	return nil
}

func (r *notificationRule) valid() []validationErr {
	var vErrs []validationErr
	if !r.endpointName.hasValue() {
		vErrs = append(vErrs, validationErr{
			Field: fieldNotificationRuleEndpointName,
			Msg:   "must be provided",
		})
	}
	if r.every == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldEvery,
			Msg:   "must be provided",
		})
	}
	if status := r.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   fmt.Sprintf("must be 1 in [active, inactive]; got=%q", r.status),
		})
	}

	if len(r.statusRules) == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldNotificationRuleStatusRules,
			Msg:   "must provide at least 1",
		})
	}

	var sRuleErrs []validationErr
	for i, sRule := range r.statusRules {
		if notification.ParseCheckLevel(sRule.curLvl) == notification.Unknown {
			sRuleErrs = append(sRuleErrs, validationErr{
				Field: fieldNotificationRuleCurrentLevel,
				Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", sRule.curLvl),
				Index: intPtr(i),
			})
		}
		if sRule.prevLvl != "" && notification.ParseCheckLevel(sRule.prevLvl) == notification.Unknown {
			sRuleErrs = append(sRuleErrs, validationErr{
				Field: fieldNotificationRulePreviousLevel,
				Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", sRule.prevLvl),
				Index: intPtr(i),
			})
		}
	}
	if len(sRuleErrs) > 0 {
		vErrs = append(vErrs, validationErr{
			Field:  fieldNotificationRuleStatusRules,
			Nested: sRuleErrs,
		})
	}

	var tagErrs []validationErr
	for i, tRule := range r.tagRules {
		if _, ok := influxdb.ToOperator(tRule.op); !ok {
			tagErrs = append(tagErrs, validationErr{
				Field: fieldOperator,
				Msg:   fmt.Sprintf("must be 1 in [equal]; got=%q", tRule.op),
				Index: intPtr(i),
			})
		}
	}
	if len(tagErrs) > 0 {
		vErrs = append(vErrs, validationErr{
			Field:  fieldNotificationRuleTagRules,
			Nested: tagErrs,
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

func toSummaryStatusRules(statusRules []struct{ curLvl, prevLvl string }) []SummaryStatusRule {
	out := make([]SummaryStatusRule, 0, len(statusRules))
	for _, sRule := range statusRules {
		out = append(out, SummaryStatusRule{
			CurrentLevel:  sRule.curLvl,
			PreviousLevel: sRule.prevLvl,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := out[i], out[j]
		if si.CurrentLevel == sj.CurrentLevel {
			return si.PreviousLevel < sj.PreviousLevel
		}
		return si.CurrentLevel < sj.CurrentLevel
	})
	return out
}

func toSummaryTagRules(tagRules []struct{ k, v, op string }) []SummaryTagRule {
	out := make([]SummaryTagRule, 0, len(tagRules))
	for _, tRule := range tagRules {
		out = append(out, SummaryTagRule{
			Key:      tRule.k,
			Value:    tRule.v,
			Operator: tRule.op,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i], out[j]
		if ti.Key == tj.Key && ti.Value == tj.Value {
			return ti.Operator < tj.Operator
		}
		if ti.Key == tj.Key {
			return ti.Value < tj.Value
		}
		return ti.Key < tj.Key
	})
	return out
}

type mapperNotificationRules []*notificationRule

func (r mapperNotificationRules) Association(i int) labelAssociater {
	return r[i]
}

func (r mapperNotificationRules) Len() int {
	return len(r)
}

const (
	fieldReferencesEnv    = "envRef"
	fieldReferencesSecret = "secretRef"
)

type references struct {
	val    interface{}
	EnvRef string
	Secret string
}

func (r *references) hasValue() bool {
	return r.EnvRef != "" || r.Secret != "" || r.val != nil
}

func (r *references) String() string {
	if r == nil {
		return ""
	}
	if v := r.StringVal(); v != "" {
		return v
	}
	if r.EnvRef != "" {
		return "$" + r.EnvRef
	}
	return ""
}

func (r *references) StringVal() string {
	if r.val != nil {
		s, _ := r.val.(string)
		return s
	}
	return ""
}

func (r *references) SecretField() influxdb.SecretField {
	if secret := r.Secret; secret != "" {
		return influxdb.SecretField{Key: secret}
	}
	if str := r.StringVal(); str != "" {
		return influxdb.SecretField{Value: &str}
	}
	return influxdb.SecretField{}
}

func isValidName(name string, minLength int) (validationErr, bool) {
	if len(name) >= minLength {
		return validationErr{}, true
	}
	return validationErr{
		Field: fieldName,
		Msg:   fmt.Sprintf("must be a string of at least %d chars in length", minLength),
	}, false
}

func toNotificationDuration(dur time.Duration) *notification.Duration {
	d, _ := notification.FromTimeDuration(dur)
	return &d
}

func durToStr(dur time.Duration) string {
	if dur == 0 {
		return ""
	}
	return dur.String()
}

func flt64Ptr(f float64) *float64 {
	if f != 0 {
		return &f
	}
	return nil
}

func intPtr(i int) *int {
	return &i
}
