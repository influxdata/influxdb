package pkger

import (
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
	ID               influxdb.ID
	Name             string
	OldDesc, NewDesc string
}

func newDiffDashboard(d *dashboard, i influxdb.Dashboard) DiffDashboard {
	return DiffDashboard{
		ID:      i.ID,
		Name:    d.Name,
		OldDesc: i.Description,
		NewDesc: d.Description,
	}
}

// IsNew indicates whether a pkg dashboard is going to be new to the platform.
func (d DiffDashboard) IsNew() bool {
	return d.ID == influxdb.ID(0)
}

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
	LabelAssociations []influxdb.Label
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
	iBkt := SummaryBucket{
		Bucket: influxdb.Bucket{
			ID:              b.ID(),
			OrgID:           b.OrgID,
			Name:            b.Name,
			Description:     b.Description,
			RetentionPeriod: b.RetentionPeriod,
		},
	}
	for _, l := range b.labels {
		iBkt.LabelAssociations = append(iBkt.LabelAssociations, influxdb.Label{
			ID:         l.ID(),
			OrgID:      l.OrgID,
			Name:       l.Name,
			Properties: l.properties(),
		})
	}
	return iBkt
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

func (l *label) ID() influxdb.ID {
	if l.existing != nil {
		return l.existing.ID
	}
	return l.id
}

func (l *label) ResourceType() influxdb.ResourceType {
	return influxdb.LabelsResourceType
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

func (l *label) setDashboardMapping(d *dashboard, exists bool) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[labelMapKey]labelMapVal)
	}

	key := labelMapKey{
		resType: influxdb.DashboardsResourceType,
		name:    d.Name,
	}
	l.mappings[key] = labelMapVal{
		exists: exists,
		v:      d,
	}
}

func (l *label) properties() map[string]string {
	return map[string]string{
		"color":       l.Color,
		"description": l.Description,
	}
}

type dashboard struct {
	id          influxdb.ID
	OrgID       influxdb.ID
	Name        string
	Description string

	labels []*label

	existing *influxdb.Dashboard
}

func (d *dashboard) ID() influxdb.ID {
	if d.existing != nil {
		return d.existing.ID
	}
	return d.id
}

func (d *dashboard) ResourceType() influxdb.ResourceType {
	return influxdb.DashboardsResourceType
}

func (d *dashboard) Exists() bool {
	return d.existing != nil
}

func (d *dashboard) summarize() SummaryDashboard {
	iDash := SummaryDashboard{
		ID:          d.ID(),
		OrgID:       d.OrgID,
		Name:        d.Name,
		Description: d.Description,
	}
	for _, l := range d.labels {
		iDash.LabelAssociations = append(iDash.LabelAssociations, influxdb.Label{
			ID:         l.ID(),
			OrgID:      l.OrgID,
			Name:       l.Name,
			Properties: l.properties(),
		})
	}
	return iDash
}
