package pkger

import (
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
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

// Kinds is a list of known pkger kinds.
func Kinds() []Kind {
	var out []Kind
	for k := range kinds {
		out = append(out, k)
	}
	return out
}

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
type SafeID platform.ID

// Encode will safely encode the id.
func (s SafeID) Encode() ([]byte, error) {
	id := platform.ID(s)
	b, _ := id.Encode()
	return b, nil
}

// String prints a encoded string representation of the id.
func (s SafeID) String() string {
	return platform.ID(s).String()
}

// DiffIdentifier are the identifying fields for any given resource. Each resource
// dictates if the resource is new, to be removed, or will remain.
type DiffIdentifier struct {
	ID          SafeID      `json:"id"`
	StateStatus StateStatus `json:"stateStatus"`
	MetaName    string      `json:"templateMetaName"`
	Kind        Kind        `json:"kind"`
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
		Name               string             `json:"name"`
		Description        string             `json:"description"`
		RetentionRules     retentionRules     `json:"retentionRules"`
		SchemaType         string             `json:"schemaType,omitempty"`
		MeasurementSchemas measurementSchemas `json:"measurementSchemas,omitempty"`
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
	if errors2.EInternal == errors2.ErrorCode(err) {
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

func (d *DiffChart) MarshalJSON() ([]byte, error) {
	return json.Marshal((*SummaryChart)(d))
}

func (d *DiffChart) UnmarshalJSON(b []byte) error {
	var sumChart SummaryChart
	if err := json.Unmarshal(b, &sumChart); err != nil {
		return err
	}
	*d = DiffChart(sumChart)
	return nil
}

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

	ResType     influxdb.ResourceType `json:"resourceType"`
	ResID       SafeID                `json:"resourceID"`
	ResName     string                `json:"resourceName"`
	ResMetaName string                `json:"resourceTemplateMetaName"`

	LabelID       SafeID `json:"labelID"`
	LabelName     string `json:"labelName"`
	LabelMetaName string `json:"labelTemplateMetaName"`
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
	if errors2.EInvalid == errors2.ErrorCode(err) {
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

// SummaryIdentifier establishes the shared identifiers for a given resource
// within a template.
type SummaryIdentifier struct {
	Kind          Kind               `json:"kind"`
	MetaName      string             `json:"templateMetaName"`
	EnvReferences []SummaryReference `json:"envReferences"`
}

// SummaryBucket provides a summary of a pkg bucket.
type SummaryBucket struct {
	SummaryIdentifier
	ID          SafeID `json:"id,omitempty"`
	OrgID       SafeID `json:"orgID,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
	// TODO: return retention rules?
	RetentionPeriod time.Duration `json:"retentionPeriod"`

	SchemaType         string                     `json:"schemaType,omitempty"`
	MeasurementSchemas []SummaryMeasurementSchema `json:"measurementSchemas,omitempty"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

type SummaryMeasurementSchema struct {
	Name    string                           `json:"name"`
	Columns []SummaryMeasurementSchemaColumn `json:"columns"`
}

type SummaryMeasurementSchemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	DataType string `json:"dataType,omitempty"`
}

// SummaryCheck provides a summary of a pkg check.
type SummaryCheck struct {
	SummaryIdentifier
	Check  influxdb.Check  `json:"check"`
	Status influxdb.Status `json:"status"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

func (s *SummaryCheck) UnmarshalJSON(b []byte) error {
	var out struct {
		SummaryIdentifier
		Status            string          `json:"status"`
		LabelAssociations []SummaryLabel  `json:"labelAssociations"`
		Check             json.RawMessage `json:"check"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return err
	}
	s.SummaryIdentifier = out.SummaryIdentifier
	s.Status = influxdb.Status(out.Status)
	s.LabelAssociations = out.LabelAssociations

	var err error
	s.Check, err = icheck.UnmarshalJSON(out.Check)
	return err
}

// SummaryDashboard provides a summary of a pkg dashboard.
type SummaryDashboard struct {
	SummaryIdentifier
	ID          SafeID         `json:"id"`
	OrgID       SafeID         `json:"orgID"`
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

// UnmarshalJSON unmarshals a view properties and other data.
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
	SummaryIdentifier
	NotificationEndpoint influxdb.NotificationEndpoint `json:"notificationEndpoint"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// UnmarshalJSON unmarshals the notificatio endpoint. This is necessary b/c of
// the notification endpoint does not have a means ot unmarshal itself.
func (s *SummaryNotificationEndpoint) UnmarshalJSON(b []byte) error {
	var a struct {
		SummaryIdentifier
		NotificationEndpoint json.RawMessage `json:"notificationEndpoint"`
		LabelAssociations    []SummaryLabel  `json:"labelAssociations"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	s.SummaryIdentifier = a.SummaryIdentifier
	s.LabelAssociations = a.LabelAssociations

	e, err := endpoint.UnmarshalJSON(a.NotificationEndpoint)
	s.NotificationEndpoint = e
	return err
}

// Summary types for NotificationRules which provide a summary of a pkg notification rule.
type (
	SummaryNotificationRule struct {
		SummaryIdentifier
		ID          SafeID `json:"id"`
		Name        string `json:"name"`
		Description string `json:"description"`

		// These fields represent the relationship of the rule to the endpoint.
		EndpointID       SafeID `json:"endpointID"`
		EndpointMetaName string `json:"endpointTemplateMetaName"`
		EndpointType     string `json:"endpointType"`

		Every           string              `json:"every"`
		Offset          string              `json:"offset"`
		MessageTemplate string              `json:"messageTemplate"`
		Status          influxdb.Status     `json:"status"`
		StatusRules     []SummaryStatusRule `json:"statusRules"`
		TagRules        []SummaryTagRule    `json:"tagRules"`

		LabelAssociations []SummaryLabel `json:"labelAssociations"`
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
	SummaryIdentifier
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
	exists           bool
	Status           StateStatus           `json:"status,omitempty"`
	ResourceID       SafeID                `json:"resourceID"`
	ResourceMetaName string                `json:"resourceTemplateMetaName"`
	ResourceName     string                `json:"resourceName"`
	ResourceType     influxdb.ResourceType `json:"resourceType"`
	LabelMetaName    string                `json:"labelTemplateMetaName"`
	LabelName        string                `json:"labelName"`
	LabelID          SafeID                `json:"labelID"`
}

// SummaryReference informs the consumer of required references for
// this resource.
type SummaryReference struct {
	Field        string      `json:"resourceField"`
	EnvRefKey    string      `json:"envRefKey"`
	ValType      string      `json:"valueType"`
	Value        interface{} `json:"value"`
	DefaultValue interface{} `json:"defaultValue"`
}

// SummaryTask provides a summary of a task.
type SummaryTask struct {
	SummaryIdentifier
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
	SummaryIdentifier
	TelegrafConfig influxdb.TelegrafConfig `json:"telegrafConfig"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// SummaryVariable provides a summary of a pkg variable.
type SummaryVariable struct {
	SummaryIdentifier
	ID          SafeID                      `json:"id,omitempty"`
	OrgID       SafeID                      `json:"orgID,omitempty"`
	Name        string                      `json:"name"`
	Description string                      `json:"description"`
	Selected    []string                    `json:"variables"`
	Arguments   *influxdb.VariableArguments `json:"arguments"`

	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}
