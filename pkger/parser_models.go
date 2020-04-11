package pkger

import (
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2"
)

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

	Description    string
	RetentionRules retentionRules
	labels         sortedLabels
}

func (b *bucket) summarize() SummaryBucket {
	return SummaryBucket{
		Name:              b.Name(),
		PkgName:           b.PkgName(),
		Description:       b.Description,
		RetentionPeriod:   b.RetentionRules.RP(),
		LabelAssociations: toSummaryLabels(b.labels...),
	}
}

func (b *bucket) ResourceType() influxdb.ResourceType {
	return KindBucket.ResourceType()
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

func (l assocMapVal) PkgName() string {
	t, ok := l.v.(interface{ PkgName() string })
	if ok {
		return t.PkgName()
	}
	return ""
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
	id influxdb.ID
	identity

	Color       string
	Description string
	associationMapping

	// exists provides context for a resource that already
	// exists in the platform. If a resource already exists(exists=true)
	// then the ID should be populated.
	existing *influxdb.Label
}

func (l *label) summarize() SummaryLabel {
	return SummaryLabel{
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
			status := StateStatusNew
			if v.exists {
				status = StateStatusExists
			}
			mappings = append(mappings, SummaryLabelMapping{
				exists:          v.exists,
				Status:          status,
				ResourceID:      SafeID(v.ID()),
				ResourcePkgName: v.PkgName(),
				ResourceName:    resource.name,
				ResourceType:    resource.resType,
				LabelID:         SafeID(l.ID()),
				LabelPkgName:    l.PkgName(),
				LabelName:       l.Name(),
			})
		}
	}

	return mappings
}

func (l *label) ID() influxdb.ID {
	if l.id != 0 {
		return l.id
	}
	if l.existing != nil {
		return l.existing.ID
	}
	return 0
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
