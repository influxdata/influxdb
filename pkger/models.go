package pkger

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb"
)

const (
	kindUnknown   kind = ""
	kindBucket    kind = "bucket"
	kindDashboard kind = "dashboard"
	kindLabel     kind = "label"
	kindPackage   kind = "package"
)

type kind string

func (k kind) String() string {
	switch k {
	case kindBucket, kindLabel, kindDashboard, kindPackage:
		return string(k)
	default:
		return "unknown"
	}
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
	Buckets       []DiffBucket
	Dashboards    []DiffDashboard
	Labels        []DiffLabel
	LabelMappings []DiffLabelMapping
}

// DiffBucket is a diff of an individual bucket.
type DiffBucket struct {
	ID                         influxdb.ID
	Name                       string
	OldDesc, NewDesc           string
	OldRetention, NewRetention time.Duration
}

// IsNew indicates whether a pkg bucket is going to be new to the platform.
func (d DiffBucket) IsNew() bool {
	return d.ID == influxdb.ID(0)
}

func newDiffBucket(b *bucket, i influxdb.Bucket) DiffBucket {
	return DiffBucket{
		ID:           i.ID,
		Name:         b.Name,
		OldDesc:      i.Description,
		NewDesc:      b.Description,
		OldRetention: i.RetentionPeriod,
		NewRetention: b.RetentionPeriod,
	}
}

// DiffDashboard is a diff of an individual dashboard.
type DiffDashboard struct {
	Name   string
	Desc   string
	Charts []DiffChart
}

func newDiffDashboard(d *dashboard) DiffDashboard {
	diff := DiffDashboard{
		Name: d.Name,
		Desc: d.Description,
	}

	for _, c := range d.Charts {
		diff.Charts = append(diff.Charts, DiffChart{
			Kind:       c.Kind,
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

// DiffLabel is a diff of an individual label.
type DiffLabel struct {
	ID                 influxdb.ID
	Name               string
	OldColor, NewColor string
	OldDesc, NewDesc   string
}

// IsNew indicates whether a pkg label is going to be new to the platform.
func (d DiffLabel) IsNew() bool {
	return d.ID == influxdb.ID(0)
}

func newDiffLabel(l *label, i influxdb.Label) DiffLabel {
	return DiffLabel{
		ID:       i.ID,
		Name:     l.Name,
		OldColor: i.Properties["color"],
		NewColor: l.Color,
		OldDesc:  i.Properties["description"],
		NewDesc:  l.Description,
	}
}

// DiffLabelMapping is a diff of an individual label mapping. A
// single resource may have multiple mappings to multiple labels.
// A label can have many mappings to other resources.
type DiffLabelMapping struct {
	IsNew bool

	ResType influxdb.ResourceType
	ResID   influxdb.ID
	ResName string

	LabelID   influxdb.ID
	LabelName string
}

// Summary is a definition of all the resources that have or
// will be created from a pkg.
type Summary struct {
	Buckets       []SummaryBucket
	Dashboards    []SummaryDashboard
	Labels        []SummaryLabel
	LabelMappings []SummaryLabelMapping
}

// SummaryBucket provides a summary of a pkg bucket.
type SummaryBucket struct {
	influxdb.Bucket
	LabelAssociations []influxdb.Label
}

// SummaryDashboard provides a summary of a pkg dashboard.
type SummaryDashboard struct {
	ID          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Description string
	Charts      []SummaryChart

	LabelAssociations []influxdb.Label
}

// ChartKind identifies what kind of chart is eluded too. Each
// chart kind has their own requirements for what constitutes
// a chart.
type ChartKind string

// available chart kinds
const (
	ChartKindUnknown    ChartKind = ""
	ChartKindSingleStat ChartKind = "single_stat"
)

func (c ChartKind) ok() bool {
	switch c {
	case ChartKindSingleStat:
		return true
	default:
		return false
	}
}

// SummaryChart provides a summary of a pkg dashboard's chart.
type SummaryChart struct {
	Kind       ChartKind
	Properties influxdb.ViewProperties

	XPosition, YPosition int
	Height, Width        int
}

// SummaryLabel provides a summary of a pkg label.
type SummaryLabel struct {
	influxdb.Label
}

// SummaryLabelMapping provides a summary of a label mapped with a single resource.
type SummaryLabelMapping struct {
	exists       bool
	ResourceName string
	LabelName    string
	influxdb.LabelMapping
}

type bucket struct {
	id              influxdb.ID
	OrgID           influxdb.ID
	Description     string
	Name            string
	RetentionPeriod time.Duration
	labels          []*label

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

func (b *bucket) ResourceType() influxdb.ResourceType {
	return influxdb.BucketsResourceType
}

func (b *bucket) Exists() bool {
	return b.existing != nil
}

func (b *bucket) summarize() SummaryBucket {
	return SummaryBucket{
		Bucket: influxdb.Bucket{
			ID:              b.ID(),
			OrgID:           b.OrgID,
			Name:            b.Name,
			Description:     b.Description,
			RetentionPeriod: b.RetentionPeriod,
		},
		LabelAssociations: toInfluxLabels(b.labels...),
	}
}

func (b *bucket) shouldApply() bool {
	return b.existing == nil ||
		b.Description != b.existing.Description ||
		b.Name != b.existing.Name ||
		b.RetentionPeriod != b.existing.RetentionPeriod
}

type labelMapKey struct {
	resType influxdb.ResourceType
	name    string
}

type labelMapVal struct {
	exists bool
	v      interface{}
}

func (l labelMapVal) bucket() (*bucket, bool) {
	if l.v == nil {
		return nil, false
	}
	b, ok := l.v.(*bucket)
	return b, ok
}

func (l labelMapVal) dashboard() (*dashboard, bool) {
	if l.v == nil {
		return nil, false
	}
	d, ok := l.v.(*dashboard)
	return d, ok
}

type label struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Color       string
	Description string

	mappings map[labelMapKey]labelMapVal

	// exists provides context for a resource that already
	// exists in the platform. If a resource already exists(exists=true)
	// then the ID should be populated.
	existing *influxdb.Label
}

func (l *label) shouldApply() bool {
	return l.existing == nil ||
		l.Description != l.existing.Properties["description"] ||
		l.Name != l.existing.Name ||
		l.Color != l.existing.Properties["color"]
}

func (l *label) ID() influxdb.ID {
	if l.existing != nil {
		return l.existing.ID
	}
	return l.id
}

func (l *label) summarize() SummaryLabel {
	return SummaryLabel{
		Label: influxdb.Label{
			ID:         l.ID(),
			OrgID:      l.OrgID,
			Name:       l.Name,
			Properties: l.properties(),
		},
	}
}

func (l *label) mappingSummary() []SummaryLabelMapping {
	var mappings []SummaryLabelMapping
	for res, lm := range l.mappings {
		mappings = append(mappings, SummaryLabelMapping{
			exists:       lm.exists,
			ResourceName: res.name,
			LabelName:    l.Name,
			LabelMapping: influxdb.LabelMapping{
				LabelID:      l.ID(),
				ResourceID:   l.getMappedResourceID(res),
				ResourceType: res.resType,
			},
		})
	}

	return mappings
}

func (l *label) getMappedResourceID(k labelMapKey) influxdb.ID {
	switch k.resType {
	case influxdb.BucketsResourceType:
		b, ok := l.mappings[k].bucket()
		if ok {
			return b.ID()
		}
	case influxdb.DashboardsResourceType:
		d, ok := l.mappings[k].dashboard()
		if ok {
			return d.ID()
		}
	}
	return 0
}

func (l *label) setBucketMapping(b *bucket, exists bool) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[labelMapKey]labelMapVal)
	}

	key := labelMapKey{
		resType: influxdb.BucketsResourceType,
		name:    b.Name,
	}
	l.mappings[key] = labelMapVal{
		exists: exists,
		v:      b,
	}
}

func (l *label) setDashboardMapping(d *dashboard) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[labelMapKey]labelMapVal)
	}

	key := labelMapKey{
		resType: d.ResourceType(),
		name:    d.Name,
	}
	l.mappings[key] = labelMapVal{v: d}
}

func (l *label) properties() map[string]string {
	return map[string]string{
		"color":       l.Color,
		"description": l.Description,
	}
}

func toInfluxLabels(labels ...*label) []influxdb.Label {
	var iLabels []influxdb.Label
	for _, l := range labels {
		iLabels = append(iLabels, influxdb.Label{
			ID:         l.ID(),
			OrgID:      l.OrgID,
			Name:       l.Name,
			Properties: l.properties(),
		})
	}
	return iLabels
}

type dashboard struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Description string
	Charts      []chart

	labels []*label
}

func (d *dashboard) ID() influxdb.ID {
	return d.id
}

func (d *dashboard) ResourceType() influxdb.ResourceType {
	return influxdb.DashboardsResourceType
}

func (d *dashboard) Exists() bool {
	return false
}

func (d *dashboard) summarize() SummaryDashboard {
	iDash := SummaryDashboard{
		ID:                d.ID(),
		OrgID:             d.OrgID,
		Name:              d.Name,
		Description:       d.Description,
		LabelAssociations: toInfluxLabels(d.labels...),
	}
	for _, c := range d.Charts {
		iDash.Charts = append(iDash.Charts, SummaryChart{
			Kind:       c.Kind,
			Properties: c.properties(),
			Height:     c.Height,
			Width:      c.Width,
			XPosition:  c.XPos,
			YPosition:  c.YPos,
		})
	}
	return iDash
}

type chart struct {
	Kind            ChartKind
	Name            string
	Prefix          string
	Suffix          string
	Note            string
	NoteOnEmpty     bool
	DecimalPlaces   int
	EnforceDecimals bool
	Shade           bool
	Colors          []*color
	Queries         []query

	XCol, YCol    string
	XPos, YPos    int
	Height, Width int
}

func (c chart) properties() influxdb.ViewProperties {
	switch c.Kind {
	case ChartKindSingleStat:
		return influxdb.SingleStatViewProperties{
			Type:   "single-stat",
			Prefix: c.Prefix,
			Suffix: c.Suffix,
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			Queries:           queries(c.Queries).influxDashQueries(),
			ViewColors:        colors(c.Colors).influxViewColors(),
		}
	default:
		return nil
	}
}

func (c chart) validProperties() []failure {
	var fails []failure

	validatorFns := []func() []failure{
		c.validBaseProps,
		queries(c.Queries).valid,
		colors(c.Colors).valid,
	}
	for _, validatorFn := range validatorFns {
		fails = append(fails, validatorFn()...)
	}

	// chart kind specific validations
	switch c.Kind {
	case ChartKindSingleStat:
		for i, clr := range c.Colors {
			if clr.Type != colorTypeText {
				fails = append(fails, failure{
					Field: fmt.Sprintf("colors[%d].type", i),
					Msg:   "single stat charts must have color type of \"text\"",
				})
			}
		}
	}

	return fails
}

func (c chart) validBaseProps() []failure {
	var fails []failure
	if c.Width <= 0 {
		fails = append(fails, failure{
			Field: "width",
			Msg:   "must be greater than 0",
		})
	}

	if c.Height <= 0 {
		fails = append(fails, failure{
			Field: "height",
			Msg:   "must be greater than 0",
		})
	}
	return fails
}

const (
	colorTypeText = "text"
)

type color struct {
	id    string
	Name  string
	Type  string
	Hex   string
	Value float64
}

// TODO:
//  - verify templates are desired
//  - template colors so references can be shared
type colors []*color

func (c colors) influxViewColors() []influxdb.ViewColor {
	var iColors []influxdb.ViewColor
	for _, cc := range c {
		iColors = append(iColors, influxdb.ViewColor{
			// need to figure out where to add this, feels best to put it in here for now
			// until we figure out what to do with sharing colors, or if that is even necessary
			ID:    cc.id,
			Type:  cc.Type,
			Hex:   cc.Hex,
			Name:  cc.Name,
			Value: cc.Value,
		})
	}
	return iColors
}

func (c colors) valid() []failure {
	var fails []failure
	if len(c) == 0 {
		fails = append(fails, failure{
			Field: "colors",
			Msg:   "at least 1 color must be provided",
		})
	}

	for i, cc := range c {
		if cc.Hex == "" {
			fails = append(fails, failure{
				Field: fmt.Sprintf("colors[%d].hex", i),
				Msg:   "a color must have a hex value provided",
			})
		}
	}

	return fails
}

type query struct {
	Query string
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

func (q queries) valid() []failure {
	var fails []failure
	if len(q) == 0 {
		fails = append(fails, failure{
			Field: "queries",
			Msg:   "at least 1 query must be provided",
		})
	}

	for i, qq := range q {
		if qq.Query == "" {
			fails = append(fails, failure{
				Field: fmt.Sprintf("queries[%d].query", i),
				Msg:   "a query must be provided",
			})
		}
	}

	return fails
}
