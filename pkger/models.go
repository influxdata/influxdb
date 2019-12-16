package pkger

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/endpoint"
)

// Package kinds.
const (
	KindUnknown                       Kind = ""
	KindBucket                        Kind = "bucket"
	KindDashboard                     Kind = "dashboard"
	KindLabel                         Kind = "label"
	KindNotificationEndpoint          Kind = "notificationendpoint"
	KindNotificationEndpointPagerDuty Kind = "notificationendpointpagerduty"
	KindNotificationEndpointHTTP      Kind = "notificationendpointhttp"
	KindNotificationEndpointSlack     Kind = "notificationendpointslack"
	KindPackage                       Kind = "package"
	KindTelegraf                      Kind = "telegraf"
	KindVariable                      Kind = "variable"
)

var kinds = map[Kind]bool{
	KindBucket:                        true,
	KindDashboard:                     true,
	KindLabel:                         true,
	KindNotificationEndpointHTTP:      true,
	KindNotificationEndpointPagerDuty: true,
	KindNotificationEndpointSlack:     true,
	KindPackage:                       true,
	KindTelegraf:                      true,
	KindVariable:                      true,
}

// Kind is a resource kind.
type Kind string

// NewKind returns the kind parsed from the provided string.
func NewKind(s string) Kind {
	return Kind(normStr(s))
}

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
	newKind := NewKind(string(k))
	if newKind == KindUnknown {
		return errors.New("invalid kind")
	}
	if !kinds[newKind] {
		return errors.New("unsupported kind provided")
	}
	return nil
}

// ResourceType converts a kind to a known resource type (if applicable).
func (k Kind) ResourceType() influxdb.ResourceType {
	switch k {
	case KindBucket:
		return influxdb.BucketsResourceType
	case KindDashboard:
		return influxdb.DashboardsResourceType
	case KindLabel:
		return influxdb.LabelsResourceType
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		return influxdb.NotificationEndpointResourceType
	case KindTelegraf:
		return influxdb.TelegrafsResourceType
	case KindVariable:
		return influxdb.VariablesResourceType
	default:
		return ""
	}
}

func (k Kind) title() string {
	return strings.Title(k.String())
}

func (k Kind) is(comps ...Kind) bool {
	normed := Kind(strings.TrimSpace(strings.ToLower(string(k))))
	for _, c := range comps {
		if c == normed {
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

// Metadata is the pkg metadata. This data describes the user
// defined identifiers.
type Metadata struct {
	Description string `yaml:"description" json:"description"`
	Name        string `yaml:"pkgName" json:"pkgName"`
	Version     string `yaml:"pkgVersion" json:"pkgVersion"`
}

// Diff is the result of a service DryRun call. The diff outlines
// what is new and or updated from the current state of the platform.
type Diff struct {
	Buckets               []DiffBucket               `json:"buckets"`
	Dashboards            []DiffDashboard            `json:"dashboards"`
	Labels                []DiffLabel                `json:"labels"`
	LabelMappings         []DiffLabelMapping         `json:"labelMappings"`
	NotificationEndpoints []DiffNotificationEndpoint `json:"notificationEndpoints"`
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
	Description    string         `json:"description"`
	RetentionRules retentionRules `json:"retentionRules"`
}

// DiffBucket is a diff of an individual bucket.
type DiffBucket struct {
	ID   SafeID            `json:"id"`
	Name string            `json:"name"`
	New  DiffBucketValues  `json:"new"`
	Old  *DiffBucketValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

func newDiffBucket(b *bucket, i *influxdb.Bucket) DiffBucket {
	diff := DiffBucket{
		Name: b.Name(),
		New: DiffBucketValues{
			Description:    b.Description,
			RetentionRules: b.RetentionRules,
		},
	}
	if i != nil {
		diff.ID = SafeID(i.ID)
		diff.Old = &DiffBucketValues{
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

// DiffDashboard is a diff of an individual dashboard.
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
	Color       string `json:"color"`
	Description string `json:"description"`
}

// DiffLabel is a diff of an individual label.
type DiffLabel struct {
	ID   SafeID           `json:"id"`
	Name string           `json:"name"`
	New  DiffLabelValues  `json:"new"`
	Old  *DiffLabelValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

// IsNew indicates whether a pkg label is going to be new to the platform.
func (d DiffLabel) IsNew() bool {
	return d.ID == SafeID(0)
}

func (d DiffLabel) hasConflict() bool {
	return d.IsNew() || d.Old != nil && *d.Old != d.New
}

func newDiffLabel(l *label, i *influxdb.Label) DiffLabel {
	diff := DiffLabel{
		Name: l.Name(),
		New: DiffLabelValues{
			Color:       l.Color,
			Description: l.Description,
		},
	}
	if i != nil {
		diff.ID = SafeID(i.ID)
		diff.Old = &DiffLabelValues{
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
func (d *DiffNotificationEndpointValues) UnmarshalJSON(b []byte) error {
	e, err := endpoint.UnmarshalJSON(b)
	if err != nil {
		fmt.Println("broken here")
	}
	d.NotificationEndpoint = e
	return err
}

// DiffNotificationEndpoint is a diff of an individual notification endpoint.
type DiffNotificationEndpoint struct {
	ID   SafeID                          `json:"id"`
	Name string                          `json:"name"`
	New  DiffNotificationEndpointValues  `json:"new"`
	Old  *DiffNotificationEndpointValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
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

// DiffTelegraf is a diff of an individual telegraf.
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
	Dashboards            []SummaryDashboard            `json:"dashboards"`
	NotificationEndpoints []SummaryNotificationEndpoint `json:"notificationEndpoints"`
	Labels                []SummaryLabel                `json:"labels"`
	LabelMappings         []SummaryLabelMapping         `json:"labelMappings"`
	TelegrafConfigs       []SummaryTelegraf             `json:"telegrafConfigs"`
	Variables             []SummaryVariable             `json:"variables"`
}

// SummaryBucket provides a summary of a pkg bucket.
type SummaryBucket struct {
	ID          SafeID `json:"id,omitempty"`
	OrgID       SafeID `json:"orgID,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
	// TODO: return retention rules?
	RetentionPeriod   time.Duration  `json:"retentionPeriod"`
	LabelAssociations []SummaryLabel `json:"labelAssociations"`
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
	chartKindXY                 chartKind = "xy"
)

func (c chartKind) ok() bool {
	switch c {
	case chartKindGauge, chartKindHeatMap, chartKindHistogram,
		chartKindMarkdown, chartKindScatter, chartKindSingleStat,
		chartKindSingleStatPlusLine, chartKindXY:
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

// SummaryNotificationEndpoint provides a summary of a pkg endpoint rule.
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

// SummaryLabel provides a summary of a pkg label.
type SummaryLabel struct {
	ID         SafeID `json:"id"`
	OrgID      SafeID `json:"orgID"`
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

const (
	fieldAssociations = "associations"
	fieldDescription  = "description"
	fieldKey          = "key"
	fieldKind         = "kind"
	fieldLanguage     = "language"
	fieldName         = "name"
	fieldPrefix       = "prefix"
	fieldQuery        = "query"
	fieldSuffix       = "suffix"
	fieldStatus       = "status"
	fieldType         = "type"
	fieldValue        = "value"
	fieldValues       = "values"
)

const (
	fieldBucketRetentionRules = "retentionRules"
)

type bucket struct {
	id             influxdb.ID
	OrgID          influxdb.ID
	Description    string
	name           string
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

func (b *bucket) Name() string {
	return b.name
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
		Description:       b.Description,
		RetentionPeriod:   b.RetentionRules.RP(),
		LabelAssociations: toSummaryLabels(b.labels...),
	}
}

func (b *bucket) valid() []validationErr {
	return b.RetentionRules.valid()
}

func (b *bucket) shouldApply() bool {
	return b.existing == nil ||
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
	if existing, ok := l.mappings[k]; ok {
		for i, ex := range existing {
			if ex.v == v {
				existing[i].exists = exists
				return
			}
		}
		l.mappings[k] = append(l.mappings[k], val)
		return
	}
	l.mappings[k] = []assocMapVal{val}
}

const (
	fieldLabelColor = "color"
)

type label struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	name        string
	Color       string
	Description string
	associationMapping

	// exists provides context for a resource that already
	// exists in the platform. If a resource already exists(exists=true)
	// then the ID should be populated.
	existing *influxdb.Label
}

func (l *label) Name() string {
	return l.name
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
		ID:    SafeID(l.ID()),
		OrgID: SafeID(l.OrgID),
		Name:  l.Name(),
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

func toSummaryLabels(labels ...*label) []SummaryLabel {
	var iLabels []SummaryLabel
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
	return s[i].name < s[j].name
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
	kind        notificationKind
	id          influxdb.ID
	OrgID       influxdb.ID
	name        string
	description string
	method      string
	password    references
	routingKey  references
	status      string
	token       references
	httpType    string
	url         string
	username    references

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

func (n *notificationEndpoint) Name() string {
	return n.name
}

func (n *notificationEndpoint) ResourceType() influxdb.ResourceType {
	return KindNotificationEndpointSlack.ResourceType()
}

func (n *notificationEndpoint) base() endpoint.Base {
	e := endpoint.Base{
		Name:        n.Name(),
		Description: n.description,
		Status:      influxdb.TaskStatusActive,
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

	if n.status != "" && influxdb.TaskStatusInactive != n.status && influxdb.TaskStatusActive != n.status {
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
	return failures
}

type mapperNotificationEndpoints []*notificationEndpoint

func (n mapperNotificationEndpoints) Association(i int) labelAssociater {
	return n[i]
}

func (n mapperNotificationEndpoints) Len() int {
	return len(n)
}

const (
	fieldTelegrafConfig = "config"
)

type telegraf struct {
	config influxdb.TelegrafConfig

	labels sortedLabels
}

func (t *telegraf) ID() influxdb.ID {
	return t.config.ID
}

func (t *telegraf) Labels() []*label {
	return t.labels
}

func (t *telegraf) Name() string {
	return t.config.Name
}

func (t *telegraf) ResourceType() influxdb.ResourceType {
	return KindTelegraf.ResourceType()
}

func (t *telegraf) Exists() bool {
	return false
}

func (t *telegraf) summarize() SummaryTelegraf {
	return SummaryTelegraf{
		TelegrafConfig:    t.config,
		LabelAssociations: toSummaryLabels(t.labels...),
	}
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
	id          influxdb.ID
	OrgID       influxdb.ID
	name        string
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

func (v *variable) Name() string {
	return v.name
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
	return failures
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

type dashboard struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	name        string
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

func (d *dashboard) Name() string {
	return d.name
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
	Suffix          string
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
	TimeFormat      string
}

func (c chart) properties() influxdb.ViewProperties {
	switch c.Kind {
	case chartKindGauge:
		return influxdb.GaugeViewProperties{
			Type:       influxdb.ViewPropertyTypeGauge,
			Queries:    c.Queries.influxDashQueries(),
			Prefix:     c.Prefix,
			Suffix:     c.Suffix,
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
			Type:   influxdb.ViewPropertyTypeSingleStat,
			Prefix: c.Prefix,
			Suffix: c.Suffix,
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
		fails = append(fails, c.Colors.hasTypes(colorTypeMin, colorTypeThreshold, colorTypeMax)...)
	case chartKindHeatMap:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindHistogram:
		fails = append(fails, c.Axes.hasAxes("x")...)
	case chartKindScatter:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindSingleStat:
		fails = append(fails, c.Colors.hasTypes(colorTypeText)...)
	case chartKindSingleStatPlusLine:
		fails = append(fails, c.Colors.hasTypes(colorTypeText)...)
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
		fails = append(fails, validPosition(c.Position)...)
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
	colorTypeMin       = "min"
	colorTypeMax       = "max"
	colorTypeScale     = "scale"
	colorTypeText      = "text"
	colorTypeThreshold = "threshold"
)

const (
	fieldColorHex = "hex"
)

type color struct {
	id   string
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
			// need to figure out where to add this, feels best to put it in here for now
			// until we figure out what to do with sharing colors, or if that is even necessary
			ID:    cc.id,
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
		// TODO: axe thsi buidler configs when issue https://github.com/influxdata/influxdb/issues/15708 is fixed up
		newQuery.BuilderConfig.Tags = append(newQuery.BuilderConfig.Tags, influxdb.NewBuilderTag("_measurement"))
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
	fieldReferencesSecret = "secretRef"
)

type references struct {
	val    interface{}
	Secret string `json:"secretRef"`
}

func (r references) hasValue() bool {
	return r.Secret != "" || r.val != nil
}

func (r references) String() string {
	if r.val != nil {
		s, _ := r.val.(string)
		return s
	}
	return ""
}

func (r references) SecretField() influxdb.SecretField {
	if secret := r.Secret; secret != "" {
		return influxdb.SecretField{Key: secret}
	}
	if str := r.String(); str != "" {
		return influxdb.SecretField{Value: &str}
	}
	return influxdb.SecretField{}
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
