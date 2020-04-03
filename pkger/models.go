package pkger

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	icheck "github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/rule"
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

// DiffBucketValues are the varying values for a bucket.
type DiffBucketValues struct {
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	RetentionRules retentionRules `json:"retentionRules"`
}

// DiffBucket is a diff of an individual bucket.
type DiffBucket struct {
	Remove  bool              `json:"remove"`
	ID      SafeID            `json:"id"`
	PkgName string            `json:"pkgName"`
	New     DiffBucketValues  `json:"new"`
	Old     *DiffBucketValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

func newDiffBucket(b *bucket, i *influxdb.Bucket) DiffBucket {
	diff := DiffBucket{
		Remove:  b.shouldRemove,
		PkgName: b.PkgName(),
		New: DiffBucketValues{
			Name:           b.Name(),
			Description:    b.Description,
			RetentionRules: b.RetentionRules,
		},
	}
	if i != nil {
		diff.ID = SafeID(i.ID)
		diff.Old = &DiffBucketValues{
			Name:        i.Name,
			Description: i.Description,
		}
		if i.RetentionPeriod > 0 {
			diff.Old.RetentionRules = retentionRules{newRetentionRule(i.RetentionPeriod)}
		}
	}
	return diff
}

// IsNew indicates whether a pkg bucket is going to be new to the platform.
func (d DiffBucket) IsNew() bool {
	return d.ID == SafeID(0)
}

func (d DiffBucket) hasConflict() bool {
	return !d.IsNew() && d.Old != nil && !reflect.DeepEqual(*d.Old, d.New)
}

// DiffCheckValues are the varying values for a check.
type DiffCheckValues struct {
	influxdb.Check
}

// UnmarshalJSON decodes the check values.
func (d *DiffCheckValues) UnmarshalJSON(b []byte) (err error) {
	d.Check, err = icheck.UnmarshalJSON(b)
	return
}

// DiffCheck is a diff of an individual check.
type DiffCheck struct {
	ID   SafeID           `json:"id"`
	Name string           `json:"name"`
	New  DiffCheckValues  `json:"new"`
	Old  *DiffCheckValues `json:"old"`
}

func newDiffCheck(c *check, iCheck influxdb.Check) DiffCheck {
	diff := DiffCheck{
		Name: c.Name(),
		New: DiffCheckValues{
			Check: c.summarize().Check,
		},
	}
	if iCheck != nil {
		diff.ID = SafeID(iCheck.GetID())
		diff.Old = &DiffCheckValues{
			Check: iCheck,
		}
	}
	return diff
}

// IsNew determines if the check in the pkg is new to the platform.
func (d DiffCheck) IsNew() bool {
	return d.Old == nil
}

// DiffDashboard is a diff of an individual dashboard. This resource is always new.
type DiffDashboard struct {
	Name   string      `json:"name"`
	Desc   string      `json:"description"`
	Charts []DiffChart `json:"charts"`
}

func newDiffDashboard(d *dashboard) DiffDashboard {
	diff := DiffDashboard{
		Name: d.Name(),
		Desc: d.Description,
	}

	for _, c := range d.Charts {
		diff.Charts = append(diff.Charts, DiffChart{
			Properties: c.properties(),
			Height:     c.Height,
			Width:      c.Width,
		})
	}

	return diff
}

// DiffChart is a diff of oa chart. Since all charts are new right now.
// the SummaryChart is reused here.
type DiffChart SummaryChart

// DiffLabelValues are the varying values for a label.
type DiffLabelValues struct {
	Name        string `json:"name"`
	Color       string `json:"color"`
	Description string `json:"description"`
}

// DiffLabel is a diff of an individual label.
type DiffLabel struct {
	Remove  bool             `json:"remove"`
	ID      SafeID           `json:"id"`
	PkgName string           `json:"pkgName"`
	New     DiffLabelValues  `json:"new"`
	Old     *DiffLabelValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

// IsNew indicates whether a pkg label is going to be new to the platform.
func (d DiffLabel) IsNew() bool {
	return d.ID == SafeID(0)
}

func (d DiffLabel) hasConflict() bool {
	return !d.IsNew() && d.Old != nil && *d.Old != d.New
}

func newDiffLabel(l *label, i *influxdb.Label) DiffLabel {
	diff := DiffLabel{
		Remove:  l.shouldRemove,
		PkgName: l.PkgName(),
		New: DiffLabelValues{
			Name:        l.Name(),
			Color:       l.Color,
			Description: l.Description,
		},
	}
	if i != nil {
		diff.ID = SafeID(i.ID)
		diff.Old = &DiffLabelValues{
			Name:        i.Name,
			Color:       i.Properties["color"],
			Description: i.Properties["description"],
		}
	}
	return diff
}

// DiffLabelMapping is a diff of an individual label mapping. A
// single resource may have multiple mappings to multiple labels.
// A label can have many mappings to other resources.
type DiffLabelMapping struct {
	IsNew bool `json:"isNew"`

	ResType influxdb.ResourceType `json:"resourceType"`
	ResID   SafeID                `json:"resourceID"`
	ResName string                `json:"resourceName"`

	LabelID   SafeID `json:"labelID"`
	LabelName string `json:"labelName"`
}

// DiffNotificationEndpointValues are the varying values for a notification endpoint.
type DiffNotificationEndpointValues struct {
	influxdb.NotificationEndpoint
}

// UnmarshalJSON decodes the notification endpoint. This is necessary unfortunately.
func (d *DiffNotificationEndpointValues) UnmarshalJSON(b []byte) (err error) {
	d.NotificationEndpoint, err = endpoint.UnmarshalJSON(b)
	return
}

// DiffNotificationEndpoint is a diff of an individual notification endpoint.
type DiffNotificationEndpoint struct {
	ID   SafeID                          `json:"id"`
	Name string                          `json:"name"`
	New  DiffNotificationEndpointValues  `json:"new"`
	Old  *DiffNotificationEndpointValues `json:"old"`
}

func newDiffNotificationEndpoint(ne *notificationEndpoint, i influxdb.NotificationEndpoint) DiffNotificationEndpoint {
	diff := DiffNotificationEndpoint{
		Name: ne.Name(),
		New: DiffNotificationEndpointValues{
			NotificationEndpoint: ne.summarize().NotificationEndpoint,
		},
	}
	if i != nil {
		diff.ID = SafeID(i.GetID())
		diff.Old = &DiffNotificationEndpointValues{
			NotificationEndpoint: i,
		}
	}
	return diff
}

// IsNew indicates if the resource will be new to the platform or if it edits
// an existing resource.
func (d DiffNotificationEndpoint) IsNew() bool {
	return d.Old == nil
}

// DiffNotificationRule is a diff of an individual notification rule. This resource is always new.
type DiffNotificationRule struct {
	Name        string `json:"name"`
	Description string `json:"description"`

	// These 3 fields represent the relationship of the rule to the endpoint.
	EndpointID   SafeID `json:"endpointID"`
	EndpointName string `json:"endpointName"`
	EndpointType string `json:"endpointType"`

	Every           string              `json:"every"`
	Offset          string              `json:"offset"`
	MessageTemplate string              `json:"messageTemplate"`
	Status          influxdb.Status     `json:"status"`
	StatusRules     []SummaryStatusRule `json:"statusRules"`
	TagRules        []SummaryTagRule    `json:"tagRules"`
}

func newDiffNotificationRule(r *notificationRule, iEndpoint influxdb.NotificationEndpoint) DiffNotificationRule {
	sum := DiffNotificationRule{
		Name:            r.Name(),
		Description:     r.description,
		EndpointName:    r.endpointName.String(),
		Every:           r.every.String(),
		Offset:          r.offset.String(),
		MessageTemplate: r.msgTemplate,
		Status:          r.Status(),
		StatusRules:     toSummaryStatusRules(r.statusRules),
		TagRules:        toSummaryTagRules(r.tagRules),
	}
	if iEndpoint != nil {
		sum.EndpointID = SafeID(iEndpoint.GetID())
		sum.EndpointType = iEndpoint.Type()
	}

	return sum
}

// DiffTask is a diff of an individual task. This resource is always new.
type DiffTask struct {
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Description string          `json:"description"`
	Every       string          `json:"every"`
	Offset      string          `json:"offset"`
	Query       string          `json:"query"`
	Status      influxdb.Status `json:"status"`
}

func newDiffTask(t *task) DiffTask {
	return DiffTask{
		Name:        t.Name(),
		Cron:        t.cron,
		Description: t.description,
		Every:       durToStr(t.every),
		Offset:      durToStr(t.offset),
		Query:       t.query,
		Status:      t.Status(),
	}
}

// DiffTelegraf is a diff of an individual telegraf. This resource is always new.
type DiffTelegraf struct {
	influxdb.TelegrafConfig
}

func newDiffTelegraf(t *telegraf) DiffTelegraf {
	return DiffTelegraf{
		TelegrafConfig: t.config,
	}
}

// DiffVariableValues are the varying values for a variable.
type DiffVariableValues struct {
	Description string                      `json:"description"`
	Args        *influxdb.VariableArguments `json:"args"`
}

// DiffVariable is a diff of an individual variable.
type DiffVariable struct {
	ID   SafeID              `json:"id"`
	Name string              `json:"name"`
	New  DiffVariableValues  `json:"new"`
	Old  *DiffVariableValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

func newDiffVariable(v *variable, iv *influxdb.Variable) DiffVariable {
	diff := DiffVariable{
		Name: v.Name(),
		New: DiffVariableValues{
			Description: v.Description,
			Args:        v.influxVarArgs(),
		},
	}
	if iv != nil {
		diff.ID = SafeID(iv.ID)
		diff.Old = &DiffVariableValues{
			Description: iv.Description,
			Args:        iv.Arguments,
		}
	}

	return diff
}

// IsNew indicates whether a pkg variable is going to be new to the platform.
func (d DiffVariable) IsNew() bool {
	return d.ID == SafeID(0)
}

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
	Check             influxdb.Check  `json:"check"`
	Status            influxdb.Status `json:"status"`
	LabelAssociations []SummaryLabel  `json:"labelAssociations"`
}

func (s *SummaryCheck) UnmarshalJSON(b []byte) error {
	var out struct {
		Status            string          `json:"status"`
		LabelAssociations []SummaryLabel  `json:"labelAssociations"`
		Check             json.RawMessage `json:"check"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return err
	}
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
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Charts      []SummaryChart `json:"charts"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// chartKind identifies what kind of chart is eluded too. Each
// chart kind has their own requirements for what constitutes
// a chart.
type chartKind string

// available chart kinds
const (
	chartKindUnknown            chartKind = ""
	chartKindGauge              chartKind = "gauge"
	chartKindHeatMap            chartKind = "heatmap"
	chartKindHistogram          chartKind = "histogram"
	chartKindMarkdown           chartKind = "markdown"
	chartKindScatter            chartKind = "scatter"
	chartKindSingleStat         chartKind = "single_stat"
	chartKindSingleStatPlusLine chartKind = "single_stat_plus_line"
	chartKindTable              chartKind = "table"
	chartKindXY                 chartKind = "xy"
)

func (c chartKind) ok() bool {
	switch c {
	case chartKindGauge, chartKindHeatMap, chartKindHistogram,
		chartKindMarkdown, chartKindScatter, chartKindSingleStat,
		chartKindSingleStatPlusLine, chartKindTable, chartKindXY:
		return true
	default:
		return false
	}
}

func (c chartKind) title() string {
	spacedKind := strings.ReplaceAll(string(c), "_", " ")
	return strings.ReplaceAll(strings.Title(spacedKind), " ", "_")
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
	NotificationEndpoint influxdb.NotificationEndpoint `json:"notificationEndpoint"`
	LabelAssociations    []SummaryLabel                `json:"labelAssociations"`
}

// UnmarshalJSON unmarshals the notificatio endpoint. This is necessary b/c of
// the notification endpoint does not have a means ot unmarshal itself.
func (s *SummaryNotificationEndpoint) UnmarshalJSON(b []byte) error {
	var a struct {
		NotificationEndpoint json.RawMessage `json:"notificationEndpoint"`
		LabelAssociations    []SummaryLabel  `json:"labelAssociations"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	s.LabelAssociations = a.LabelAssociations

	e, err := endpoint.UnmarshalJSON(a.NotificationEndpoint)
	s.NotificationEndpoint = e
	return err
}

// Summary types for NotificationRules which provide a summary of a pkg notification rule.
type (
	SummaryNotificationRule struct {
		ID          SafeID `json:"id"`
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
	exists       bool
	ResourceID   SafeID                `json:"resourceID"`
	ResourceName string                `json:"resourceName"`
	ResourceType influxdb.ResourceType `json:"resourceType"`
	LabelName    string                `json:"labelName"`
	LabelID      SafeID                `json:"labelID"`
}

// SummaryTask provides a summary of a task.
type SummaryTask struct {
	ID          SafeID          `json:"id"`
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
	TelegrafConfig    influxdb.TelegrafConfig `json:"telegrafConfig"`
	LabelAssociations []SummaryLabel          `json:"labelAssociations"`
}

// SummaryVariable provides a summary of a pkg variable.
type SummaryVariable struct {
	ID                SafeID                      `json:"id,omitempty"`
	OrgID             SafeID                      `json:"orgID,omitempty"`
	Name              string                      `json:"name"`
	Description       string                      `json:"description"`
	Arguments         *influxdb.VariableArguments `json:"arguments"`
	LabelAssociations []SummaryLabel              `json:"labelAssociations"`
}

type identity struct {
	name         *references
	displayName  *references
	shouldRemove bool
}

func (i *identity) Name() string {
	if displayName := i.displayName.String(); displayName != "" {
		return displayName
	}
	return i.name.String()
}

func (i *identity) PkgName() string {
	return i.name.String()
}

const (
	fieldAPIVersion   = "apiVersion"
	fieldAssociations = "associations"
	fieldDescription  = "description"
	fieldEvery        = "every"
	fieldKey          = "key"
	fieldKind         = "kind"
	fieldLanguage     = "language"
	fieldLevel        = "level"
	fieldMin          = "min"
	fieldMax          = "max"
	fieldMetadata     = "metadata"
	fieldName         = "name"
	fieldOffset       = "offset"
	fieldOperator     = "operator"
	fieldPrefix       = "prefix"
	fieldQuery        = "query"
	fieldSuffix       = "suffix"
	fieldSpec         = "spec"
	fieldStatus       = "status"
	fieldType         = "type"
	fieldValue        = "value"
	fieldValues       = "values"
)

const (
	fieldBucketRetentionRules = "retentionRules"
)

const bucketNameMinLength = 2

type bucket struct {
	identity

	id             influxdb.ID
	OrgID          influxdb.ID
	Description    string
	RetentionRules retentionRules
	labels         sortedLabels

	// existing provides context for a resource that already
	// exists in the platform. If a resource already exists
	// then it will be referenced here.
	existing *influxdb.Bucket
}

func (b *bucket) ID() influxdb.ID {
	if b.existing != nil {
		return b.existing.ID
	}
	return b.id
}

func (b *bucket) Labels() []*label {
	return b.labels
}

func (b *bucket) ResourceType() influxdb.ResourceType {
	return KindBucket.ResourceType()
}

func (b *bucket) Exists() bool {
	return b.existing != nil
}

func (b *bucket) summarize() SummaryBucket {
	return SummaryBucket{
		ID:                SafeID(b.ID()),
		OrgID:             SafeID(b.OrgID),
		Name:              b.Name(),
		PkgName:           b.PkgName(),
		Description:       b.Description,
		RetentionPeriod:   b.RetentionRules.RP(),
		LabelAssociations: toSummaryLabels(b.labels...),
	}
}

func (b *bucket) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(b.Name(), bucketNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	vErrs = append(vErrs, b.RetentionRules.valid()...)
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

func (b *bucket) shouldApply() bool {
	return b.shouldRemove ||
		b.existing == nil ||
		b.Description != b.existing.Description ||
		b.Name() != b.existing.Name ||
		b.RetentionRules.RP() != b.existing.RetentionPeriod
}

type mapperBuckets []*bucket

func (b mapperBuckets) Association(i int) labelAssociater {
	return b[i]
}

func (b mapperBuckets) Len() int {
	return len(b)
}

const (
	retentionRuleTypeExpire = "expire"
)

type retentionRule struct {
	Type    string `json:"type" yaml:"type"`
	Seconds int    `json:"everySeconds" yaml:"everySeconds"`
}

func newRetentionRule(d time.Duration) retentionRule {
	return retentionRule{
		Type:    retentionRuleTypeExpire,
		Seconds: int(d.Round(time.Second) / time.Second),
	}
}

func (r retentionRule) valid() []validationErr {
	const hour = 3600
	var ff []validationErr
	if r.Seconds < hour {
		ff = append(ff, validationErr{
			Field: fieldRetentionRulesEverySeconds,
			Msg:   "seconds must be a minimum of " + strconv.Itoa(hour),
		})
	}
	if r.Type != retentionRuleTypeExpire {
		ff = append(ff, validationErr{
			Field: fieldType,
			Msg:   `type must be "expire"`,
		})
	}
	return ff
}

const (
	fieldRetentionRulesEverySeconds = "everySeconds"
)

type retentionRules []retentionRule

func (r retentionRules) RP() time.Duration {
	// TODO: this feels very odd to me, will need to follow up with
	//  team to better understand this
	for _, rule := range r {
		return time.Duration(rule.Seconds) * time.Second
	}
	return 0
}

func (r retentionRules) valid() []validationErr {
	var failures []validationErr
	for i, rule := range r {
		if ff := rule.valid(); len(ff) > 0 {
			failures = append(failures, validationErr{
				Field:  fieldBucketRetentionRules,
				Index:  intPtr(i),
				Nested: ff,
			})
		}
	}
	return failures
}

type checkKind int

const (
	checkKindDeadman checkKind = iota + 1
	checkKindThreshold
)

const (
	fieldCheckAllValues             = "allValues"
	fieldCheckReportZero            = "reportZero"
	fieldCheckStaleTime             = "staleTime"
	fieldCheckStatusMessageTemplate = "statusMessageTemplate"
	fieldCheckTags                  = "tags"
	fieldCheckThresholds            = "thresholds"
	fieldCheckTimeSince             = "timeSince"
)

const checkNameMinLength = 1

type check struct {
	identity

	id            influxdb.ID
	orgID         influxdb.ID
	kind          checkKind
	description   string
	every         time.Duration
	level         string
	offset        time.Duration
	query         string
	reportZero    bool
	staleTime     time.Duration
	status        string
	statusMessage string
	tags          []struct{ k, v string }
	timeSince     time.Duration
	thresholds    []threshold

	labels sortedLabels

	existing influxdb.Check
}

func (c *check) Exists() bool {
	return c.existing != nil
}

func (c *check) ID() influxdb.ID {
	if c.existing != nil {
		return c.existing.GetID()
	}
	return c.id
}

func (c *check) Labels() []*label {
	return c.labels
}

func (c *check) ResourceType() influxdb.ResourceType {
	return KindCheck.ResourceType()
}

func (c *check) Status() influxdb.Status {
	status := influxdb.Status(c.status)
	if status == "" {
		status = influxdb.Active
	}
	return status
}

func (c *check) summarize() SummaryCheck {
	base := icheck.Base{
		ID:                    c.ID(),
		OrgID:                 c.orgID,
		Name:                  c.Name(),
		Description:           c.description,
		Every:                 toNotificationDuration(c.every),
		Offset:                toNotificationDuration(c.offset),
		StatusMessageTemplate: c.statusMessage,
	}
	base.Query.Text = c.query
	for _, tag := range c.tags {
		base.Tags = append(base.Tags, influxdb.Tag{Key: tag.k, Value: tag.v})
	}

	sum := SummaryCheck{
		Status:            c.Status(),
		LabelAssociations: toSummaryLabels(c.labels...),
	}
	switch c.kind {
	case checkKindThreshold:
		sum.Check = &icheck.Threshold{
			Base:       base,
			Thresholds: toInfluxThresholds(c.thresholds...),
		}
	case checkKindDeadman:
		sum.Check = &icheck.Deadman{
			Base:       base,
			Level:      notification.ParseCheckLevel(strings.ToUpper(c.level)),
			ReportZero: c.reportZero,
			StaleTime:  toNotificationDuration(c.staleTime),
			TimeSince:  toNotificationDuration(c.timeSince),
		}
	}
	return sum
}

func (c *check) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(c.Name(), checkNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if c.every == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldEvery,
			Msg:   "duration value must be provided that is >= 5s (seconds)",
		})
	}
	if c.query == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldQuery,
			Msg:   "must provide a non zero value",
		})
	}
	if c.statusMessage == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldCheckStatusMessageTemplate,
			Msg:   `must provide a template; ex. "Check: ${ r._check_name } is: ${ r._level }"`,
		})
	}
	if status := c.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   "must be 1 of [active, inactive]",
		})
	}

	switch c.kind {
	case checkKindThreshold:
		if len(c.thresholds) == 0 {
			vErrs = append(vErrs, validationErr{
				Field: fieldCheckThresholds,
				Msg:   "must provide at least 1 threshold entry",
			})
		}
		for i, th := range c.thresholds {
			for _, fail := range th.valid() {
				fail.Index = intPtr(i)
				vErrs = append(vErrs, fail)
			}
		}
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

type mapperChecks []*check

func (c mapperChecks) Association(i int) labelAssociater {
	return c[i]
}

func (c mapperChecks) Len() int {
	return len(c)
}

type thresholdType string

const (
	thresholdTypeGreater      thresholdType = "greater"
	thresholdTypeLesser       thresholdType = "lesser"
	thresholdTypeInsideRange  thresholdType = "inside_range"
	thresholdTypeOutsideRange thresholdType = "outside_range"
)

var thresholdTypes = map[thresholdType]bool{
	thresholdTypeGreater:      true,
	thresholdTypeLesser:       true,
	thresholdTypeInsideRange:  true,
	thresholdTypeOutsideRange: true,
}

type threshold struct {
	threshType thresholdType
	allVals    bool
	level      string
	val        float64
	min, max   float64
}

func (t threshold) valid() []validationErr {
	var vErrs []validationErr
	if notification.ParseCheckLevel(t.level) == notification.Unknown {
		vErrs = append(vErrs, validationErr{
			Field: fieldLevel,
			Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", t.level),
		})
	}
	if !thresholdTypes[t.threshType] {
		vErrs = append(vErrs, validationErr{
			Field: fieldType,
			Msg:   fmt.Sprintf("must be 1 in [Lesser, Greater, Inside_Range, Outside_Range]; got=%q", t.threshType),
		})
	}
	if t.min > t.max {
		vErrs = append(vErrs, validationErr{
			Field: fieldMin,
			Msg:   "min must be < max",
		})
	}
	return vErrs
}

func toInfluxThresholds(thresholds ...threshold) []icheck.ThresholdConfig {
	var iThresh []icheck.ThresholdConfig
	for _, th := range thresholds {
		base := icheck.ThresholdConfigBase{
			AllValues: th.allVals,
			Level:     notification.ParseCheckLevel(th.level),
		}
		switch th.threshType {
		case thresholdTypeGreater:
			iThresh = append(iThresh, icheck.Greater{
				ThresholdConfigBase: base,
				Value:               th.val,
			})
		case thresholdTypeLesser:
			iThresh = append(iThresh, icheck.Lesser{
				ThresholdConfigBase: base,
				Value:               th.val,
			})
		case thresholdTypeInsideRange, thresholdTypeOutsideRange:
			iThresh = append(iThresh, icheck.Range{
				ThresholdConfigBase: base,
				Max:                 th.max,
				Min:                 th.min,
				Within:              th.threshType == thresholdTypeInsideRange,
			})
		}
	}
	return iThresh
}

type assocMapKey struct {
	resType influxdb.ResourceType
	name    string
}

type assocMapVal struct {
	exists bool
	v      interface{}
}

func (l assocMapVal) ID() influxdb.ID {
	if t, ok := l.v.(labelAssociater); ok {
		return t.ID()
	}
	return 0
}

type associationMapping struct {
	mappings map[assocMapKey][]assocMapVal
}

func (l *associationMapping) setMapping(v interface {
	ResourceType() influxdb.ResourceType
	Name() string
}, exists bool) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[assocMapKey][]assocMapVal)
	}

	k := assocMapKey{
		resType: v.ResourceType(),
		name:    v.Name(),
	}
	val := assocMapVal{
		exists: exists,
		v:      v,
	}
	existing, ok := l.mappings[k]
	if !ok {
		l.mappings[k] = []assocMapVal{val}
		return
	}
	for i, ex := range existing {
		if ex.v == v {
			existing[i].exists = exists
			return
		}
	}
	l.mappings[k] = append(l.mappings[k], val)
}

const (
	fieldLabelColor = "color"
)

const labelNameMinLength = 2

type label struct {
	identity

	id          influxdb.ID
	OrgID       influxdb.ID
	Color       string
	Description string
	associationMapping

	// exists provides context for a resource that already
	// exists in the platform. If a resource already exists(exists=true)
	// then the ID should be populated.
	existing *influxdb.Label
}

func (l *label) ID() influxdb.ID {
	if l.existing != nil {
		return l.existing.ID
	}
	return l.id
}

func (l *label) shouldApply() bool {
	return l.existing == nil ||
		l.Description != l.existing.Properties["description"] ||
		l.Name() != l.existing.Name ||
		l.Color != l.existing.Properties["color"]
}

func (l *label) summarize() SummaryLabel {
	return SummaryLabel{
		ID:      SafeID(l.ID()),
		OrgID:   SafeID(l.OrgID),
		PkgName: l.PkgName(),
		Name:    l.Name(),
		Properties: struct {
			Color       string `json:"color"`
			Description string `json:"description"`
		}{
			Color:       l.Color,
			Description: l.Description,
		},
	}
}

func (l *label) mappingSummary() []SummaryLabelMapping {
	var mappings []SummaryLabelMapping
	for resource, vals := range l.mappings {
		for _, v := range vals {
			mappings = append(mappings, SummaryLabelMapping{
				exists:       v.exists,
				ResourceID:   SafeID(v.ID()),
				ResourceName: resource.name,
				ResourceType: resource.resType,
				LabelID:      SafeID(l.ID()),
				LabelName:    l.Name(),
			})
		}
	}

	return mappings
}

func (l *label) properties() map[string]string {
	return map[string]string{
		"color":       l.Color,
		"description": l.Description,
	}
}

func (l *label) toInfluxLabel() influxdb.Label {
	return influxdb.Label{
		ID:         l.ID(),
		OrgID:      l.OrgID,
		Name:       l.Name(),
		Properties: l.properties(),
	}
}

func (l *label) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(l.Name(), labelNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

func toSummaryLabels(labels ...*label) []SummaryLabel {
	iLabels := make([]SummaryLabel, 0, len(labels))
	for _, l := range labels {
		iLabels = append(iLabels, l.summarize())
	}
	return iLabels
}

type sortedLabels []*label

func (s sortedLabels) Len() int {
	return len(s)
}

func (s sortedLabels) Less(i, j int) bool {
	return s[i].Name() < s[j].Name()
}

func (s sortedLabels) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type notificationKind int

const (
	notificationKindHTTP notificationKind = iota + 1
	notificationKindPagerDuty
	notificationKindSlack
)

const (
	notificationHTTPAuthTypeBasic  = "basic"
	notificationHTTPAuthTypeBearer = "bearer"
	notificationHTTPAuthTypeNone   = "none"
)

const (
	fieldNotificationEndpointHTTPMethod = "method"
	fieldNotificationEndpointPassword   = "password"
	fieldNotificationEndpointRoutingKey = "routingKey"
	fieldNotificationEndpointToken      = "token"
	fieldNotificationEndpointURL        = "url"
	fieldNotificationEndpointUsername   = "username"
)

type notificationEndpoint struct {
	identity

	kind        notificationKind
	id          influxdb.ID
	OrgID       influxdb.ID
	description string
	method      string
	password    *references
	routingKey  *references
	status      string
	token       *references
	httpType    string
	url         string
	username    *references

	labels sortedLabels

	existing influxdb.NotificationEndpoint
}

func (n *notificationEndpoint) Exists() bool {
	return n.existing != nil
}

func (n *notificationEndpoint) ID() influxdb.ID {
	if n.existing != nil {
		return n.existing.GetID()
	}
	return n.id
}

func (n *notificationEndpoint) Labels() []*label {
	return n.labels
}

func (n *notificationEndpoint) ResourceType() influxdb.ResourceType {
	return KindNotificationEndpointSlack.ResourceType()
}

func (n *notificationEndpoint) base() endpoint.Base {
	e := endpoint.Base{
		Name:        n.Name(),
		Description: n.description,
		Status:      influxdb.Active,
	}
	if id := n.ID(); id > 0 {
		e.ID = &id
	}
	if orgID := n.OrgID; orgID > 0 {
		e.OrgID = &orgID
	}
	return e
}

func (n *notificationEndpoint) summarize() SummaryNotificationEndpoint {
	base := n.base()
	if n.status != "" {
		base.Status = influxdb.Status(n.status)
	}
	sum := SummaryNotificationEndpoint{
		LabelAssociations: toSummaryLabels(n.labels...),
	}

	switch n.kind {
	case notificationKindHTTP:
		e := &endpoint.HTTP{
			Base:   base,
			URL:    n.url,
			Method: n.method,
		}
		switch n.httpType {
		case notificationHTTPAuthTypeBasic:
			e.AuthMethod = notificationHTTPAuthTypeBasic
			e.Password = n.password.SecretField()
			e.Username = n.username.SecretField()
		case notificationHTTPAuthTypeBearer:
			e.AuthMethod = notificationHTTPAuthTypeBearer
			e.Token = n.token.SecretField()
		case notificationHTTPAuthTypeNone:
			e.AuthMethod = notificationHTTPAuthTypeNone
		}
		sum.NotificationEndpoint = e
	case notificationKindPagerDuty:
		sum.NotificationEndpoint = &endpoint.PagerDuty{
			Base:       base,
			ClientURL:  n.url,
			RoutingKey: n.routingKey.SecretField(),
		}
	case notificationKindSlack:
		sum.NotificationEndpoint = &endpoint.Slack{
			Base:  base,
			URL:   n.url,
			Token: n.token.SecretField(),
		}
	}
	return sum
}

var validEndpointHTTPMethods = map[string]bool{
	"DELETE":  true,
	"GET":     true,
	"HEAD":    true,
	"OPTIONS": true,
	"PATCH":   true,
	"POST":    true,
	"PUT":     true,
}

func (n *notificationEndpoint) valid() []validationErr {
	var failures []validationErr
	if _, err := url.Parse(n.url); err != nil || n.url == "" {
		failures = append(failures, validationErr{
			Field: fieldNotificationEndpointURL,
			Msg:   "must be valid url",
		})
	}

	status := influxdb.Status(n.status)
	if status != "" && influxdb.Inactive != status && influxdb.Active != status {
		failures = append(failures, validationErr{
			Field: fieldStatus,
			Msg:   "not a valid status; valid statues are one of [active, inactive]",
		})
	}

	switch n.kind {
	case notificationKindPagerDuty:
		if !n.routingKey.hasValue() {
			failures = append(failures, validationErr{
				Field: fieldNotificationEndpointRoutingKey,
				Msg:   "must be provide",
			})
		}
	case notificationKindHTTP:
		if !validEndpointHTTPMethods[n.method] {
			failures = append(failures, validationErr{
				Field: fieldNotificationEndpointHTTPMethod,
				Msg:   "http method must be a valid HTTP verb",
			})
		}

		switch n.httpType {
		case notificationHTTPAuthTypeBasic:
			if !n.password.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointPassword,
					Msg:   "must provide non empty string",
				})
			}
			if !n.username.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointUsername,
					Msg:   "must provide non empty string",
				})
			}
		case notificationHTTPAuthTypeBearer:
			if !n.token.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointToken,
					Msg:   "must provide non empty string",
				})
			}
		case notificationHTTPAuthTypeNone:
		default:
			failures = append(failures, validationErr{
				Field: fieldType,
				Msg: fmt.Sprintf(
					"invalid type provided %q; valid type is 1 in [%s, %s, %s]",
					n.httpType,
					notificationHTTPAuthTypeBasic,
					notificationHTTPAuthTypeBearer,
					notificationHTTPAuthTypeNone,
				),
			})
		}
	}

	if len(failures) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, failures...),
		}
	}

	return nil
}

type mapperNotificationEndpoints []*notificationEndpoint

func (n mapperNotificationEndpoints) Association(i int) labelAssociater {
	return n[i]
}

func (n mapperNotificationEndpoints) Len() int {
	return len(n)
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

	labels sortedLabels
}

func (r *notificationRule) Exists() bool {
	return false
}

func (r *notificationRule) ID() influxdb.ID {
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
	fieldTaskCron = "cron"
)

type task struct {
	identity

	id          influxdb.ID
	orgID       influxdb.ID
	cron        string
	description string
	every       time.Duration
	offset      time.Duration
	query       string
	status      string

	labels sortedLabels
}

func (t *task) Exists() bool {
	return false
}

func (t *task) ID() influxdb.ID {
	return t.id
}

func (t *task) Labels() []*label {
	return t.labels
}

func (t *task) ResourceType() influxdb.ResourceType {
	return KindTask.ResourceType()
}

func (t *task) Status() influxdb.Status {
	if t.status == "" {
		return influxdb.Active
	}
	return influxdb.Status(t.status)
}

var fluxRegex = regexp.MustCompile(`import\s+\".*\"`)

func (t *task) flux() string {
	taskOpts := []string{fmt.Sprintf("name: %q", t.Name())}
	if t.cron != "" {
		taskOpts = append(taskOpts, fmt.Sprintf("cron: %q", t.cron))
	}
	if t.every > 0 {
		taskOpts = append(taskOpts, fmt.Sprintf("every: %s", t.every))
	}
	if t.offset > 0 {
		taskOpts = append(taskOpts, fmt.Sprintf("offset: %s", t.offset))
	}

	// this is required by the API, super nasty. Will be super challenging for
	// anyone outside org to figure out how to do this within an hour of looking
	// at the API :sadpanda:. Would be ideal to let the API translate the arguments
	// into this required form instead of forcing that complexity on the caller.
	taskOptStr := fmt.Sprintf("\noption task = { %s }", strings.Join(taskOpts, ", "))

	if indices := fluxRegex.FindAllIndex([]byte(t.query), -1); len(indices) > 0 {
		lastImportIdx := indices[len(indices)-1][1]
		pieces := append([]string{},
			t.query[:lastImportIdx],
			taskOptStr,
			t.query[lastImportIdx:],
		)
		return fmt.Sprint(strings.Join(pieces, "\n"))
	}

	return fmt.Sprintf("%s\n%s", taskOptStr, t.query)
}

func (t *task) summarize() SummaryTask {
	return SummaryTask{
		ID:          SafeID(t.ID()),
		Name:        t.Name(),
		Cron:        t.cron,
		Description: t.description,
		Every:       durToStr(t.every),
		Offset:      durToStr(t.offset),
		Query:       t.query,
		Status:      t.Status(),

		LabelAssociations: toSummaryLabels(t.labels...),
	}
}

func (t *task) valid() []validationErr {
	var vErrs []validationErr
	if t.cron == "" && t.every == 0 {
		vErrs = append(vErrs,
			validationErr{
				Field: fieldEvery,
				Msg:   "must provide if cron field is not provided",
			},
			validationErr{
				Field: fieldTaskCron,
				Msg:   "must provide if every field is not provided",
			},
		)
	}

	if t.query == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldQuery,
			Msg:   "must provide a non zero value",
		})
	}

	if status := t.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   "must be 1 of [active, inactive]",
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

type mapperTasks []*task

func (m mapperTasks) Association(i int) labelAssociater {
	return m[i]
}

func (m mapperTasks) Len() int {
	return len(m)
}

const (
	fieldTelegrafConfig = "config"
)

type telegraf struct {
	identity

	config influxdb.TelegrafConfig

	labels sortedLabels
}

func (t *telegraf) ID() influxdb.ID {
	return t.config.ID
}

func (t *telegraf) Labels() []*label {
	return t.labels
}

func (t *telegraf) ResourceType() influxdb.ResourceType {
	return KindTelegraf.ResourceType()
}

func (t *telegraf) Exists() bool {
	return false
}

func (t *telegraf) summarize() SummaryTelegraf {
	cfg := t.config
	cfg.Name = t.Name()
	return SummaryTelegraf{
		TelegrafConfig:    cfg,
		LabelAssociations: toSummaryLabels(t.labels...),
	}
}

func (t *telegraf) valid() []validationErr {
	var vErrs []validationErr
	if t.config.Config == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldTelegrafConfig,
			Msg:   "no config provided",
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

type mapperTelegrafs []*telegraf

func (m mapperTelegrafs) Association(i int) labelAssociater {
	return m[i]
}

func (m mapperTelegrafs) Len() int {
	return len(m)
}

const (
	fieldArgTypeConstant = "constant"
	fieldArgTypeMap      = "map"
	fieldArgTypeQuery    = "query"
)

type variable struct {
	identity

	id          influxdb.ID
	OrgID       influxdb.ID
	Description string
	Type        string
	Query       string
	Language    string
	ConstValues []string
	MapValues   map[string]string

	labels sortedLabels

	existing *influxdb.Variable
}

func (v *variable) ID() influxdb.ID {
	if v.existing != nil {
		return v.existing.ID
	}
	return v.id
}

func (v *variable) Exists() bool {
	return v.existing != nil
}

func (v *variable) Labels() []*label {
	return v.labels
}

func (v *variable) ResourceType() influxdb.ResourceType {
	return KindVariable.ResourceType()
}

func (v *variable) shouldApply() bool {
	return v.existing == nil ||
		v.existing.Description != v.Description ||
		v.existing.Arguments == nil ||
		!reflect.DeepEqual(v.existing.Arguments, v.influxVarArgs())
}

func (v *variable) summarize() SummaryVariable {
	return SummaryVariable{
		ID:                SafeID(v.ID()),
		OrgID:             SafeID(v.OrgID),
		Name:              v.Name(),
		Description:       v.Description,
		Arguments:         v.influxVarArgs(),
		LabelAssociations: toSummaryLabels(v.labels...),
	}
}

func (v *variable) influxVarArgs() *influxdb.VariableArguments {
	args := &influxdb.VariableArguments{
		Type: v.Type,
	}
	switch args.Type {
	case "query":
		args.Values = influxdb.VariableQueryValues{
			Query:    v.Query,
			Language: v.Language,
		}
	case "constant":
		args.Values = influxdb.VariableConstantValues(v.ConstValues)
	case "map":
		args.Values = influxdb.VariableMapValues(v.MapValues)
	}
	return args
}

func (v *variable) valid() []validationErr {
	var failures []validationErr
	switch v.Type {
	case "map":
		if len(v.MapValues) == 0 {
			failures = append(failures, validationErr{
				Field: fieldValues,
				Msg:   "map variable must have at least 1 key/val pair",
			})
		}
	case "constant":
		if len(v.ConstValues) == 0 {
			failures = append(failures, validationErr{
				Field: fieldValues,
				Msg:   "constant variable must have a least 1 value provided",
			})
		}
	case "query":
		if v.Query == "" {
			failures = append(failures, validationErr{
				Field: fieldQuery,
				Msg:   "query variable must provide a query string",
			})
		}
		if v.Language != "influxql" && v.Language != "flux" {
			failures = append(failures, validationErr{
				Field: fieldLanguage,
				Msg:   fmt.Sprintf(`query variable language must be either "influxql" or "flux"; got %q`, v.Language),
			})
		}
	}
	if len(failures) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, failures...),
		}
	}

	return nil
}

type mapperVariables []*variable

func (m mapperVariables) Association(i int) labelAssociater {
	return m[i]
}

func (m mapperVariables) Len() int {
	return len(m)
}

const (
	fieldDashCharts = "charts"
)

const dashboardNameMinLength = 2

type dashboard struct {
	identity

	id          influxdb.ID
	OrgID       influxdb.ID
	Description string
	Charts      []chart

	labels sortedLabels
}

func (d *dashboard) ID() influxdb.ID {
	return d.id
}

func (d *dashboard) Labels() []*label {
	return d.labels
}

func (d *dashboard) ResourceType() influxdb.ResourceType {
	return KindDashboard.ResourceType()
}

func (d *dashboard) Exists() bool {
	return false
}

func (d *dashboard) summarize() SummaryDashboard {
	iDash := SummaryDashboard{
		ID:                SafeID(d.ID()),
		OrgID:             SafeID(d.OrgID),
		Name:              d.Name(),
		Description:       d.Description,
		LabelAssociations: toSummaryLabels(d.labels...),
	}
	for _, c := range d.Charts {
		iDash.Charts = append(iDash.Charts, SummaryChart{
			Properties: c.properties(),
			Height:     c.Height,
			Width:      c.Width,
			XPosition:  c.XPos,
			YPosition:  c.YPos,
		})
	}
	return iDash
}

func (d *dashboard) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(d.Name(), dashboardNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

type mapperDashboards []*dashboard

func (m mapperDashboards) Association(i int) labelAssociater {
	return m[i]
}

func (m mapperDashboards) Len() int {
	return len(m)
}

const (
	fieldChartAxes          = "axes"
	fieldChartBinCount      = "binCount"
	fieldChartBinSize       = "binSize"
	fieldChartColors        = "colors"
	fieldChartDecimalPlaces = "decimalPlaces"
	fieldChartDomain        = "domain"
	fieldChartGeom          = "geom"
	fieldChartHeight        = "height"
	fieldChartLegend        = "legend"
	fieldChartNote          = "note"
	fieldChartNoteOnEmpty   = "noteOnEmpty"
	fieldChartPosition      = "position"
	fieldChartQueries       = "queries"
	fieldChartShade         = "shade"
	fieldChartFieldOptions  = "fieldOptions"
	fieldChartTableOptions  = "tableOptions"
	fieldChartTickPrefix    = "tickPrefix"
	fieldChartTickSuffix    = "tickSuffix"
	fieldChartTimeFormat    = "timeFormat"
	fieldChartWidth         = "width"
	fieldChartXCol          = "xCol"
	fieldChartXPos          = "xPos"
	fieldChartYCol          = "yCol"
	fieldChartYPos          = "yPos"
)

type chart struct {
	Kind            chartKind
	Name            string
	Prefix          string
	TickPrefix      string
	Suffix          string
	TickSuffix      string
	Note            string
	NoteOnEmpty     bool
	DecimalPlaces   int
	EnforceDecimals bool
	Shade           bool
	Legend          legend
	Colors          colors
	Queries         queries
	Axes            axes
	Geom            string
	XCol, YCol      string
	XPos, YPos      int
	Height, Width   int
	BinSize         int
	BinCount        int
	Position        string
	FieldOptions    []fieldOption
	TableOptions    tableOptions
	TimeFormat      string
}

func (c chart) properties() influxdb.ViewProperties {
	switch c.Kind {
	case chartKindGauge:
		return influxdb.GaugeViewProperties{
			Type:       influxdb.ViewPropertyTypeGauge,
			Queries:    c.Queries.influxDashQueries(),
			Prefix:     c.Prefix,
			TickPrefix: c.TickPrefix,
			Suffix:     c.Suffix,
			TickSuffix: c.TickSuffix,
			ViewColors: c.Colors.influxViewColors(),
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
		}
	case chartKindHeatMap:
		return influxdb.HeatmapViewProperties{
			Type:              influxdb.ViewPropertyTypeHeatMap,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.strings(),
			BinSize:           int32(c.BinSize),
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			XDomain:           c.Axes.get("x").Domain,
			YDomain:           c.Axes.get("y").Domain,
			XPrefix:           c.Axes.get("x").Prefix,
			YPrefix:           c.Axes.get("y").Prefix,
			XSuffix:           c.Axes.get("x").Suffix,
			YSuffix:           c.Axes.get("y").Suffix,
			XAxisLabel:        c.Axes.get("x").Label,
			YAxisLabel:        c.Axes.get("y").Label,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			TimeFormat:        c.TimeFormat,
		}
	case chartKindHistogram:
		return influxdb.HistogramViewProperties{
			Type:              influxdb.ViewPropertyTypeHistogram,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.influxViewColors(),
			FillColumns:       []string{},
			XColumn:           c.XCol,
			XDomain:           c.Axes.get("x").Domain,
			XAxisLabel:        c.Axes.get("x").Label,
			Position:          c.Position,
			BinCount:          c.BinCount,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
		}
	case chartKindMarkdown:
		return influxdb.MarkdownViewProperties{
			Type: influxdb.ViewPropertyTypeMarkdown,
			Note: c.Note,
		}
	case chartKindScatter:
		return influxdb.ScatterViewProperties{
			Type:              influxdb.ViewPropertyTypeScatter,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.strings(),
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			XDomain:           c.Axes.get("x").Domain,
			YDomain:           c.Axes.get("y").Domain,
			XPrefix:           c.Axes.get("x").Prefix,
			YPrefix:           c.Axes.get("y").Prefix,
			XSuffix:           c.Axes.get("x").Suffix,
			YSuffix:           c.Axes.get("y").Suffix,
			XAxisLabel:        c.Axes.get("x").Label,
			YAxisLabel:        c.Axes.get("y").Label,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			TimeFormat:        c.TimeFormat,
		}
	case chartKindSingleStat:
		return influxdb.SingleStatViewProperties{
			Type:       influxdb.ViewPropertyTypeSingleStat,
			Prefix:     c.Prefix,
			TickPrefix: c.TickPrefix,
			Suffix:     c.Suffix,
			TickSuffix: c.TickSuffix,
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.influxViewColors(),
		}
	case chartKindSingleStatPlusLine:
		return influxdb.LinePlusSingleStatProperties{
			Type:   influxdb.ViewPropertyTypeSingleStatPlusLine,
			Prefix: c.Prefix,
			Suffix: c.Suffix,
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			ShadeBelow:        c.Shade,
			Legend:            c.Legend.influxLegend(),
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.influxViewColors(),
			Axes:              c.Axes.influxAxes(),
			Position:          c.Position,
		}
	case chartKindTable:
		fieldOptions := make([]influxdb.RenamableField, 0, len(c.FieldOptions))
		for _, fieldOpt := range c.FieldOptions {
			fieldOptions = append(fieldOptions, influxdb.RenamableField{
				InternalName: fieldOpt.FieldName,
				DisplayName:  fieldOpt.DisplayName,
				Visible:      fieldOpt.Visible,
			})
		}
		sort.Slice(fieldOptions, func(i, j int) bool {
			return fieldOptions[i].InternalName < fieldOptions[j].InternalName
		})

		return influxdb.TableViewProperties{
			Type:              influxdb.ViewPropertyTypeTable,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Queries:    c.Queries.influxDashQueries(),
			ViewColors: c.Colors.influxViewColors(),
			TableOptions: influxdb.TableOptions{
				VerticalTimeAxis: c.TableOptions.VerticalTimeAxis,
				SortBy: influxdb.RenamableField{
					InternalName: c.TableOptions.SortByField,
				},
				Wrapping:       c.TableOptions.Wrapping,
				FixFirstColumn: c.TableOptions.FixFirstColumn,
			},
			FieldOptions: fieldOptions,
			TimeFormat:   c.TimeFormat,
		}
	case chartKindXY:
		return influxdb.XYViewProperties{
			Type:              influxdb.ViewPropertyTypeXY,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			ShadeBelow:        c.Shade,
			Legend:            c.Legend.influxLegend(),
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.influxViewColors(),
			Axes:              c.Axes.influxAxes(),
			Geom:              c.Geom,
			Position:          c.Position,
			TimeFormat:        c.TimeFormat,
		}
	default:
		return nil
	}
}

func (c chart) validProperties() []validationErr {
	if c.Kind == chartKindMarkdown {
		// at the time of writing, there's nothing to validate for markdown types
		return nil
	}

	var fails []validationErr

	validatorFns := []func() []validationErr{
		c.validBaseProps,
		c.Queries.valid,
		c.Colors.valid,
	}
	for _, validatorFn := range validatorFns {
		fails = append(fails, validatorFn()...)
	}

	// chart kind specific validations
	switch c.Kind {
	case chartKindGauge:
		fails = append(fails, c.Colors.hasTypes(colorTypeMin, colorTypeMax)...)
	case chartKindHeatMap:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindHistogram:
		fails = append(fails, c.Axes.hasAxes("x")...)
	case chartKindScatter:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindSingleStat:
	case chartKindSingleStatPlusLine:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
		fails = append(fails, validPosition(c.Position)...)
	case chartKindTable:
		fails = append(fails, validTableOptions(c.TableOptions)...)
	case chartKindXY:
		fails = append(fails, validGeometry(c.Geom)...)
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
		fails = append(fails, validPosition(c.Position)...)
	}

	return fails
}

func validPosition(pos string) []validationErr {
	pos = strings.ToLower(pos)
	if pos != "" && pos != "overlaid" && pos != "stacked" {
		return []validationErr{{
			Field: fieldChartPosition,
			Msg:   fmt.Sprintf("invalid position supplied %q; valid positions is one of [overlaid, stacked]", pos),
		}}
	}
	return nil
}

var geometryTypes = map[string]bool{
	"line":    true,
	"step":    true,
	"stacked": true,
	"bar":     true,
}

func validGeometry(geom string) []validationErr {
	if !geometryTypes[geom] {
		msg := "type not found"
		if geom != "" {
			msg = "type provided is not supported"
		}
		return []validationErr{{
			Field: fieldChartGeom,
			Msg:   fmt.Sprintf("%s: %q", msg, geom),
		}}
	}

	return nil
}

func (c chart) validBaseProps() []validationErr {
	var fails []validationErr
	if c.Width <= 0 {
		fails = append(fails, validationErr{
			Field: fieldChartWidth,
			Msg:   "must be greater than 0",
		})
	}

	if c.Height <= 0 {
		fails = append(fails, validationErr{
			Field: fieldChartHeight,
			Msg:   "must be greater than 0",
		})
	}
	return fails
}

const (
	fieldChartFieldOptionDisplayName = "displayName"
	fieldChartFieldOptionFieldName   = "fieldName"
	fieldChartFieldOptionVisible     = "visible"
)

type fieldOption struct {
	FieldName   string
	DisplayName string
	Visible     bool
}

const (
	fieldChartTableOptionVerticalTimeAxis = "verticalTimeAxis"
	fieldChartTableOptionSortBy           = "sortBy"
	fieldChartTableOptionWrapping         = "wrapping"
	fieldChartTableOptionFixFirstColumn   = "fixFirstColumn"
)

type tableOptions struct {
	VerticalTimeAxis bool
	SortByField      string
	Wrapping         string
	FixFirstColumn   bool
}

func validTableOptions(opts tableOptions) []validationErr {
	var fails []validationErr

	switch opts.Wrapping {
	case "", "single-line", "truncate", "wrap":
	default:
		fails = append(fails, validationErr{
			Field: fieldChartTableOptionWrapping,
			Msg:   `chart table option should 1 in ["single-line", "truncate", "wrap"]`,
		})
	}

	if len(fails) == 0 {
		return nil
	}

	return []validationErr{
		{
			Field:  fieldChartTableOptions,
			Nested: fails,
		},
	}
}

const (
	colorTypeBackground = "background"
	colorTypeMin        = "min"
	colorTypeMax        = "max"
	colorTypeScale      = "scale"
	colorTypeText       = "text"
	colorTypeThreshold  = "threshold"
)

const (
	fieldColorHex = "hex"
)

type color struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
	Type string `json:"type,omitempty" yaml:"type,omitempty"`
	Hex  string `json:"hex,omitempty" yaml:"hex,omitempty"`
	// using reference for Value here so we can set to nil and
	// it will be ignored during encoding, keeps our exported pkgs
	// clear of unneeded entries.
	Value *float64 `json:"value,omitempty" yaml:"value,omitempty"`
}

// TODO:
//  - verify templates are desired
//  - template colors so references can be shared
type colors []*color

func (c colors) influxViewColors() []influxdb.ViewColor {
	ptrToFloat64 := func(f *float64) float64 {
		if f == nil {
			return 0
		}
		return *f
	}

	var iColors []influxdb.ViewColor
	for _, cc := range c {
		iColors = append(iColors, influxdb.ViewColor{
			Type:  cc.Type,
			Hex:   cc.Hex,
			Name:  cc.Name,
			Value: ptrToFloat64(cc.Value),
		})
	}
	return iColors
}

func (c colors) strings() []string {
	clrs := []string{}

	for _, clr := range c {
		clrs = append(clrs, clr.Hex)
	}

	return clrs
}

// TODO: looks like much of these are actually getting defaults in
//  the UI. looking at sytem charts, seeign lots of failures for missing
//  color types or no colors at all.
func (c colors) hasTypes(types ...string) []validationErr {
	tMap := make(map[string]bool)
	for _, cc := range c {
		tMap[cc.Type] = true
	}

	var failures []validationErr
	for _, t := range types {
		if !tMap[t] {
			failures = append(failures, validationErr{
				Field: "colors",
				Msg:   fmt.Sprintf("type not found: %q", t),
			})
		}
	}

	return failures
}

func (c colors) valid() []validationErr {
	var fails []validationErr
	for i, cc := range c {
		cErr := validationErr{
			Field: fieldChartColors,
			Index: intPtr(i),
		}
		if cc.Hex == "" {
			cErr.Nested = append(cErr.Nested, validationErr{
				Field: fieldColorHex,
				Msg:   "a color must have a hex value provided",
			})
		}
		if len(cErr.Nested) > 0 {
			fails = append(fails, cErr)
		}
	}

	return fails
}

type query struct {
	Query string `json:"query" yaml:"query"`
}

type queries []query

func (q queries) influxDashQueries() []influxdb.DashboardQuery {
	var iQueries []influxdb.DashboardQuery
	for _, qq := range q {
		newQuery := influxdb.DashboardQuery{
			Text:     qq.Query,
			EditMode: "advanced",
		}
		// TODO: axe this builder configs when issue https://github.com/influxdata/influxdb/issues/15708 is fixed up
		newQuery.BuilderConfig.Tags = append(newQuery.BuilderConfig.Tags, influxdb.NewBuilderTag("_measurement", "filter", ""))
		iQueries = append(iQueries, newQuery)
	}
	return iQueries
}

func (q queries) valid() []validationErr {
	var fails []validationErr
	if len(q) == 0 {
		fails = append(fails, validationErr{
			Field: fieldChartQueries,
			Msg:   "at least 1 query must be provided",
		})
	}

	for i, qq := range q {
		qErr := validationErr{
			Field: fieldChartQueries,
			Index: intPtr(i),
		}
		if qq.Query == "" {
			qErr.Nested = append(fails, validationErr{
				Field: fieldQuery,
				Msg:   "a query must be provided",
			})
		}
		if len(qErr.Nested) > 0 {
			fails = append(fails, qErr)
		}
	}

	return fails
}

const (
	fieldAxisBase  = "base"
	fieldAxisLabel = "label"
	fieldAxisScale = "scale"
)

type axis struct {
	Base   string    `json:"base,omitempty" yaml:"base,omitempty"`
	Label  string    `json:"label,omitempty" yaml:"label,omitempty"`
	Name   string    `json:"name,omitempty" yaml:"name,omitempty"`
	Prefix string    `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Scale  string    `json:"scale,omitempty" yaml:"scale,omitempty"`
	Suffix string    `json:"suffix,omitempty" yaml:"suffix,omitempty"`
	Domain []float64 `json:"domain,omitempty" yaml:"domain,omitempty"`
}

type axes []axis

func (a axes) get(name string) axis {
	for _, ax := range a {
		if name == ax.Name {
			return ax
		}
	}
	return axis{}
}

func (a axes) influxAxes() map[string]influxdb.Axis {
	m := make(map[string]influxdb.Axis)
	for _, ax := range a {
		m[ax.Name] = influxdb.Axis{
			Bounds: []string{},
			Label:  ax.Label,
			Prefix: ax.Prefix,
			Suffix: ax.Suffix,
			Base:   ax.Base,
			Scale:  ax.Scale,
		}
	}
	return m
}

func (a axes) hasAxes(expectedAxes ...string) []validationErr {
	mAxes := make(map[string]bool)
	for _, ax := range a {
		mAxes[ax.Name] = true
	}

	var failures []validationErr
	for _, expected := range expectedAxes {
		if !mAxes[expected] {
			failures = append(failures, validationErr{
				Field: fieldChartAxes,
				Msg:   fmt.Sprintf("axis not found: %q", expected),
			})
		}
	}

	return failures
}

const (
	fieldLegendOrientation = "orientation"
)

type legend struct {
	Orientation string `json:"orientation,omitempty" yaml:"orientation,omitempty"`
	Type        string `json:"type" yaml:"type"`
}

func (l legend) influxLegend() influxdb.Legend {
	return influxdb.Legend{
		Type:        l.Type,
		Orientation: l.Orientation,
	}
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
