package pkger

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
)

// Package kinds.
const (
	KindUnknown   Kind = ""
	KindBucket    Kind = "bucket"
	KindDashboard Kind = "dashboard"
	KindLabel     Kind = "label"
	KindPackage   Kind = "package"
	KindVariable  Kind = "variable"
)

var kinds = map[Kind]bool{
	KindBucket:    true,
	KindDashboard: true,
	KindLabel:     true,
	KindPackage:   true,
	KindVariable:  true,
}

// Kind is a resource kind.
type Kind string

func newKind(s string) Kind {
	return Kind(strings.TrimSpace(strings.ToLower(s)))
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
	newKind := Kind(strings.ToLower(string(k)))
	if newKind == KindUnknown {
		return errors.New("invalid kind")
	}
	if !kinds[newKind] {
		return errors.New("unsupported kind provided")
	}
	return nil
}

func (k Kind) title() string {
	return strings.Title(k.String())
}

func (k Kind) is(comp Kind) bool {
	normed := Kind(strings.TrimSpace(strings.ToLower(string(k))))
	return normed == comp
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
	Buckets       []DiffBucket       `json:"buckets"`
	Dashboards    []DiffDashboard    `json:"dashboards"`
	Labels        []DiffLabel        `json:"labels"`
	LabelMappings []DiffLabelMapping `json:"labelMappings"`
	Variables     []DiffVariable     `json:"variables"`
}

// DiffBucket is a diff of an individual bucket.
type DiffBucket struct {
	ID           SafeID        `json:"id"`
	Name         string        `json:"name"`
	OldDesc      string        `json:"oldDescription"`
	NewDesc      string        `json:"newDescription"`
	OldRetention time.Duration `json:"oldRP"`
	NewRetention time.Duration `json:"newRP"`
}

// IsNew indicates whether a pkg bucket is going to be new to the platform.
func (d DiffBucket) IsNew() bool {
	return d.ID == SafeID(0)
}

func newDiffBucket(b *bucket, i influxdb.Bucket) DiffBucket {
	return DiffBucket{
		ID:           SafeID(i.ID),
		Name:         b.Name,
		OldDesc:      i.Description,
		NewDesc:      b.Description,
		OldRetention: i.RetentionPeriod,
		NewRetention: b.RetentionPeriod,
	}
}

// DiffDashboard is a diff of an individual dashboard.
type DiffDashboard struct {
	Name   string      `json:"name"`
	Desc   string      `json:"description"`
	Charts []DiffChart `json:"charts"`
}

func newDiffDashboard(d *dashboard) DiffDashboard {
	diff := DiffDashboard{
		Name: d.Name,
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

// DiffLabel is a diff of an individual label.
type DiffLabel struct {
	ID       SafeID `json:"id"`
	Name     string `json:"name"`
	OldColor string `json:"oldColor"`
	NewColor string `json:"newColor"`
	OldDesc  string `json:"oldDescription"`
	NewDesc  string `json:"newDescription"`
}

// IsNew indicates whether a pkg label is going to be new to the platform.
func (d DiffLabel) IsNew() bool {
	return d.ID == SafeID(0)
}

func newDiffLabel(l *label, i influxdb.Label) DiffLabel {
	return DiffLabel{
		ID:       SafeID(i.ID),
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
	IsNew bool `json:"isNew"`

	ResType influxdb.ResourceType `json:"resourceType"`
	ResID   SafeID                `json:"resourceID"`
	ResName string                `json:"resourceName"`

	LabelID   SafeID `json:"labelID"`
	LabelName string `json:"labelName"`
}

// DiffVariable is a diff of an individual variable.
type DiffVariable struct {
	ID      SafeID `json:"id"`
	Name    string `json:"name"`
	OldDesc string `json:"oldDescription"`
	NewDesc string `json:"newDescription"`

	OldArgs *influxdb.VariableArguments `json:"oldArgs"`
	NewArgs *influxdb.VariableArguments `json:"newArgs"`
}

func newDiffVariable(v *variable, iv influxdb.Variable) DiffVariable {
	return DiffVariable{
		ID:      SafeID(iv.ID),
		Name:    v.Name,
		OldDesc: iv.Description,
		NewDesc: v.Description,
		OldArgs: iv.Arguments,
		NewArgs: v.influxVarArgs(),
	}
}

// IsNew indicates whether a pkg variable is going to be new to the platform.
func (d DiffVariable) IsNew() bool {
	return d.ID == SafeID(0)
}

// Summary is a definition of all the resources that have or
// will be created from a pkg.
type Summary struct {
	Buckets       []SummaryBucket       `json:"buckets"`
	Dashboards    []SummaryDashboard    `json:"dashboards"`
	Labels        []SummaryLabel        `json:"labels"`
	LabelMappings []SummaryLabelMapping `json:"labelMappings"`
	Variables     []SummaryVariable     `json:"variables"`
}

// SummaryBucket provides a summary of a pkg bucket.
type SummaryBucket struct {
	influxdb.Bucket
	LabelAssociations []influxdb.Label `json:"labelAssociations"`
}

// SummaryDashboard provides a summary of a pkg dashboard.
type SummaryDashboard struct {
	ID          SafeID         `json:"id"`
	OrgID       SafeID         `json:"orgID"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Charts      []SummaryChart `json:"charts"`

	LabelAssociations []influxdb.Label `json:"labelAssociations"`
}

// chartKind identifies what kind of chart is eluded too. Each
// chart kind has their own requirements for what constitutes
// a chart.
type chartKind string

// available chart kinds
const (
	chartKindUnknown            chartKind = ""
	chartKindGauge              chartKind = "gauge"
	chartKindSingleStat         chartKind = "single_stat"
	chartKindSingleStatPlusLine chartKind = "single_stat_plus_line"
	chartKindXY                 chartKind = "xy"
)

func (c chartKind) ok() bool {
	switch c {
	case chartKindSingleStat, chartKindSingleStatPlusLine, chartKindXY,
		chartKindGauge:
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
	Properties influxdb.ViewProperties `json:"properties"`

	XPosition int `json:"xPos"`
	YPosition int `json:"yPos"`
	Height    int `json:"height"`
	Width     int `json:"width"`
}

// SummaryLabel provides a summary of a pkg label.
type SummaryLabel struct {
	influxdb.Label
}

// SummaryLabelMapping provides a summary of a label mapped with a single resource.
type SummaryLabelMapping struct {
	exists       bool
	ResourceName string `json:"resourceName"`
	LabelName    string `json:"labelName"`
	influxdb.LabelMapping
}

// SummaryVariable provides a summary of a pkg variable.
type SummaryVariable struct {
	influxdb.Variable
	LabelAssociations []influxdb.Label `json:"labelAssociations"`
}

const (
	fieldAssociations = "associations"
	fieldDescription  = "description"
	fieldKind         = "kind"
	fieldName         = "name"
	fieldPrefix       = "prefix"
	fieldQuery        = "query"
	fieldSuffix       = "suffix"
	fieldType         = "type"
	fieldValue        = "value"
	fieldValues       = "values"
)

const (
	fieldBucketRetentionPeriod = "retention_period"
)

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

type assocMapKey struct {
	resType influxdb.ResourceType
	name    string
}

type assocMapVal struct {
	exists bool
	v      interface{}
}

func (l assocMapVal) bucket() (*bucket, bool) {
	if l.v == nil {
		return nil, false
	}
	b, ok := l.v.(*bucket)
	return b, ok
}

func (l assocMapVal) dashboard() (*dashboard, bool) {
	if l.v == nil {
		return nil, false
	}
	d, ok := l.v.(*dashboard)
	return d, ok
}

func (l assocMapVal) variable() (*variable, bool) {
	if l.v == nil {
		return nil, false
	}
	v, ok := l.v.(*variable)
	return v, ok
}

type associationMapping struct {
	mappings map[assocMapKey]assocMapVal
}

func (l *associationMapping) setMapping(k assocMapKey, v assocMapVal) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[assocMapKey]assocMapVal)
	}
	l.mappings[k] = v
}

func (l *associationMapping) setBucketMapping(b *bucket, exists bool) {
	key := assocMapKey{
		resType: b.ResourceType(),
		name:    b.Name,
	}
	val := assocMapVal{
		exists: exists,
		v:      b,
	}
	l.setMapping(key, val)
}

func (l *associationMapping) setDashboardMapping(d *dashboard) {
	key := assocMapKey{
		resType: d.ResourceType(),
		name:    d.Name,
	}
	val := assocMapVal{v: d}
	l.setMapping(key, val)
}

func (l *associationMapping) setVariableMapping(v *variable, exists bool) {
	key := assocMapKey{
		resType: v.ResourceType(),
		name:    v.Name,
	}
	val := assocMapVal{
		exists: exists,
		v:      v,
	}
	l.setMapping(key, val)
}

const (
	fieldLabelColor = "color"
)

type label struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Color       string
	Description string
	associationMapping

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

func (l *label) getMappedResourceID(k assocMapKey) influxdb.ID {
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
	case influxdb.VariablesResourceType:
		v, ok := l.mappings[k].variable()
		if ok {
			return v.ID()
		}
	}
	return 0
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

const (
	fieldArgTypeConstant = "constant"
	fieldArgTypeMap      = "map"
	fieldArgTypeQuery    = "query"
	fieldVarLanguage     = "language"
)

type variable struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Description string
	Type        string
	Query       string
	Language    string
	ConstValues []string
	MapValues   map[string]string

	labels []*label

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

func (v *variable) ResourceType() influxdb.ResourceType {
	return influxdb.VariablesResourceType
}

func (v *variable) shouldApply() bool {
	return v.existing == nil ||
		v.existing.Description != v.Description ||
		v.existing.Arguments == nil ||
		v.existing.Arguments.Type != v.Type
}

func (v *variable) summarize() SummaryVariable {
	return SummaryVariable{
		Variable: influxdb.Variable{
			ID:             v.ID(),
			OrganizationID: v.OrgID,
			Name:           v.Name,
			Description:    v.Description,
			Arguments:      v.influxVarArgs(),
		},
		LabelAssociations: toInfluxLabels(v.labels...),
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

func (v *variable) valid() []failure {
	var failures []failure
	switch v.Type {
	case "map":
		if len(v.MapValues) == 0 {
			failures = append(failures, failure{
				Field: "values",
				Msg:   "map variable must have at least 1 key/val pair",
			})
		}
	case "constant":
		if len(v.ConstValues) == 0 {
			failures = append(failures, failure{
				Field: "values",
				Msg:   "constant variable must have a least 1 value provided",
			})
		}
	case "query":
		if v.Query == "" {
			failures = append(failures, failure{
				Field: "query",
				Msg:   "query variable must provide a query string",
			})
		}
		if v.Language != "influxql" && v.Language != "flux" {
			const msgFmt = "query variable language must be either %q or %q; got %q"
			failures = append(failures, failure{
				Field: "language",
				Msg:   fmt.Sprintf(msgFmt, "influxql", "flux", v.Language),
			})
		}
	}
	return failures
}

const (
	fieldDashCharts = "charts"
)

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
		ID:                SafeID(d.ID()),
		OrgID:             SafeID(d.OrgID),
		Name:              d.Name,
		Description:       d.Description,
		LabelAssociations: toInfluxLabels(d.labels...),
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

const (
	fieldChartAxes          = "axes"
	fieldChartColors        = "colors"
	fieldChartDecimalPlaces = "decimalPlaces"
	fieldChartGeom          = "geom"
	fieldChartHeight        = "height"
	fieldChartLegend        = "legend"
	fieldChartNote          = "note"
	fieldChartNoteOnEmpty   = "noteOnEmpty"
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

	XCol, YCol    string
	XPos, YPos    int
	Height, Width int
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
		}
	default:
		return nil
	}
}

func (c chart) validProperties() []failure {
	var fails []failure

	validatorFns := []func() []failure{
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
	case chartKindSingleStat:
		fails = append(fails, c.Colors.hasTypes(colorTypeText)...)
	case chartKindSingleStatPlusLine:
		fails = append(fails, c.Colors.hasTypes(colorTypeText)...)
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindXY:
		fails = append(fails, validGeometry(c.Geom)...)
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	}

	return fails
}

var geometryTypes = map[string]bool{
	"line":    true,
	"step":    true,
	"stacked": true,
	"bar":     true,
}

func validGeometry(geom string) []failure {
	if !geometryTypes[geom] {
		msg := "type not found"
		if geom != "" {
			msg = "type provided is not supported"
		}
		return []failure{{
			Field: "geom",
			Msg:   fmt.Sprintf("%s: %q", msg, geom),
		}}
	}

	return nil
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

// TODO: looks like much of these are actually getting defaults in
//  the UI. looking at sytem charts, seeign lots of failures for missing
//  color types or no colors at all.
func (c colors) hasTypes(types ...string) []failure {
	tMap := make(map[string]bool)
	for _, cc := range c {
		tMap[cc.Type] = true
	}

	var failures []failure
	for _, t := range types {
		if !tMap[t] {
			failures = append(failures, failure{
				Field: "colors",
				Msg:   fmt.Sprintf("type not found: %q", t),
			})
		}
	}

	return failures
}

func (c colors) valid() []failure {
	var fails []failure
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

const (
	fieldAxisBase  = "base"
	fieldAxisLabel = "label"
	fieldAxisScale = "scale"
)

type axis struct {
	Base   string `json:"base,omitempty" yaml:"base,omitempty"`
	Label  string `json:"label,omitempty" yaml:"label,omitempty"`
	Name   string `json:"name,omitempty" yaml:"name,omitempty"`
	Prefix string `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Scale  string `json:"scale,omitempty" yaml:"scale,omitempty"`
	Suffix string `json:"suffix,omitempty" yaml:"suffix,omitempty"`
}

type axes []axis

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

func (a axes) hasAxes(expectedAxes ...string) []failure {
	mAxes := make(map[string]bool)
	for _, ax := range a {
		mAxes[ax.Name] = true
	}

	var failures []failure
	for _, expected := range expectedAxes {
		if !mAxes[expected] {
			failures = append(failures, failure{
				Field: "axes",
				Msg:   fmt.Sprintf("axis not found: %q", expected),
			})
		}
	}

	return failures
}

const (
	fieldLegendLanguage    = "language"
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

func flt64Ptr(f float64) *float64 {
	if f != 0 {
		return &f
	}
	return nil
}
