package pkger

import (
	"sort"

	"github.com/influxdata/influxdb/v2"
)

type stateCoordinator struct {
	mBuckets map[string]*stateBucket

	mLabels map[string]*stateLabel

	labelMappings []stateLabelMapping
}

func newStateCoordinator(pkg *Pkg) *stateCoordinator {
	state := stateCoordinator{
		mBuckets: make(map[string]*stateBucket),
		mLabels:  make(map[string]*stateLabel),
	}

	for _, pkgBkt := range pkg.buckets() {
		state.mBuckets[pkgBkt.PkgName()] = &stateBucket{
			bucket: pkgBkt,
		}
	}

	for _, pkgLabel := range pkg.labels() {
		state.mLabels[pkgLabel.PkgName()] = &stateLabel{
			label: pkgLabel,
		}
	}

	return &state
}

func (s *stateCoordinator) buckets() []*stateBucket {
	out := make([]*stateBucket, 0, len(s.mBuckets))
	for _, v := range s.mBuckets {
		out = append(out, v)
	}
	return out
}

func (s *stateCoordinator) labels() []*stateLabel {
	out := make([]*stateLabel, 0, len(s.mLabels))
	for _, v := range s.mLabels {
		out = append(out, v)
	}
	return out
}

func (s *stateCoordinator) diff() Diff {
	var diff Diff
	for _, b := range s.mBuckets {
		diff.Buckets = append(diff.Buckets, b.diffBucket())
	}
	sort.Slice(diff.Buckets, func(i, j int) bool {
		return diff.Buckets[i].PkgName < diff.Buckets[j].PkgName
	})

	for _, l := range s.mLabels {
		diff.Labels = append(diff.Labels, l.diffLabel())
	}
	sort.Slice(diff.Labels, func(i, j int) bool {
		return diff.Labels[i].PkgName < diff.Labels[j].PkgName
	})

	for _, m := range s.labelMappings {
		diff.LabelMappings = append(diff.LabelMappings, m.diffLabelMapping())
	}
	sort.Slice(diff.LabelMappings, func(i, j int) bool {
		n, m := diff.LabelMappings[i], diff.LabelMappings[j]
		if n.ResType < m.ResType {
			return true
		}
		if n.ResType > m.ResType {
			return false
		}
		if n.ResPkgName < m.ResPkgName {
			return true
		}
		if n.ResPkgName > m.ResPkgName {
			return false
		}
		return n.LabelName < m.LabelName
	})

	return diff
}

func (s *stateCoordinator) summary() Summary {
	var sum Summary
	for _, b := range s.buckets() {
		if b.shouldRemove {
			continue
		}
		sum.Buckets = append(sum.Buckets, b.summarize())
	}
	sort.Slice(sum.Buckets, func(i, j int) bool {
		return sum.Buckets[i].PkgName < sum.Buckets[j].PkgName
	})

	for _, l := range s.labels() {
		if l.shouldRemove {
			continue
		}
		sum.Labels = append(sum.Labels, l.summarize())
	}
	sort.Slice(sum.Labels, func(i, j int) bool {
		return sum.Labels[i].PkgName < sum.Labels[j].PkgName
	})

	for _, m := range s.labelMappings {
		sum.LabelMappings = append(sum.LabelMappings, m.summarize())
	}
	sort.Slice(sum.LabelMappings, func(i, j int) bool {
		n, m := sum.LabelMappings[i], sum.LabelMappings[j]
		if n.ResourceType != m.ResourceType {
			return n.ResourceType < m.ResourceType
		}
		if n.ResourcePkgName != m.ResourcePkgName {
			return n.ResourcePkgName < m.ResourcePkgName
		}
		return n.LabelName < m.LabelName
	})

	return sum
}

func (s *stateCoordinator) getLabelByPkgName(pkgName string) *stateLabel {
	return s.mLabels[pkgName]
}

func (s *stateCoordinator) Contains(k Kind, pkgName string) bool {
	_, ok := s.getObjectIDSetter(k, pkgName)
	return ok
}

// setObjectID sets the id for the resource graphed from the object the key identifies.
func (s *stateCoordinator) setObjectID(k Kind, pkgName string, id influxdb.ID) {
	idSetFn, ok := s.getObjectIDSetter(k, pkgName)
	if !ok {
		return
	}
	idSetFn(id)
}

// setObjectID sets the id for the resource graphed from the object the key identifies.
// The pkgName and kind are used as the unique identifier, when calling this it will
// overwrite any existing value if one exists. If desired, check for the value by using
// the Contains method.
func (s *stateCoordinator) addObjectForRemoval(k Kind, pkgName string, id influxdb.ID) {
	newIdentity := identity{
		name: &references{val: pkgName},
	}

	switch k {
	case KindBucket:
		s.mBuckets[pkgName] = &stateBucket{
			id:           id,
			bucket:       &bucket{identity: newIdentity},
			shouldRemove: true,
		}
	case KindLabel:
		s.mLabels[pkgName] = &stateLabel{
			id:           id,
			label:        &label{identity: newIdentity},
			shouldRemove: true,
		}
	}
}

func (s *stateCoordinator) getObjectIDSetter(k Kind, pkgName string) (func(influxdb.ID), bool) {
	switch k {
	case KindBucket:
		r, ok := s.mBuckets[pkgName]
		return func(id influxdb.ID) {
			r.id = id
		}, ok
	case KindLabel:
		r, ok := s.mLabels[pkgName]
		return func(id influxdb.ID) {
			r.id = id
		}, ok
	default:
		return nil, false
	}
}

type stateIdentity struct {
	id           influxdb.ID
	name         string
	pkgName      string
	resourceType influxdb.ResourceType
	shouldRemove bool
}

func (s stateIdentity) exists() bool {
	return s.id != 0
}

type stateBucket struct {
	id, orgID    influxdb.ID
	shouldRemove bool

	existing *influxdb.Bucket

	*bucket
}

func (b *stateBucket) diffBucket() DiffBucket {
	diff := DiffBucket{
		DiffIdentifier: DiffIdentifier{
			ID:      SafeID(b.ID()),
			Remove:  b.shouldRemove,
			PkgName: b.PkgName(),
		},
		New: DiffBucketValues{
			Name:           b.Name(),
			Description:    b.Description,
			RetentionRules: b.RetentionRules,
		},
	}
	if e := b.existing; e != nil {
		diff.Old = &DiffBucketValues{
			Name:        e.Name,
			Description: e.Description,
		}
		if e.RetentionPeriod > 0 {
			diff.Old.RetentionRules = retentionRules{newRetentionRule(e.RetentionPeriod)}
		}
	}
	return diff
}

func (b *stateBucket) summarize() SummaryBucket {
	sum := b.bucket.summarize()
	sum.ID = SafeID(b.ID())
	sum.OrgID = SafeID(b.orgID)
	return sum
}

func (b *stateBucket) Exists() bool {
	return b.existing != nil
}

func (b *stateBucket) ID() influxdb.ID {
	if b.Exists() {
		return b.existing.ID
	}
	return b.id
}

func (b *stateBucket) resourceType() influxdb.ResourceType {
	return KindBucket.ResourceType()
}

func (b *stateBucket) labels() []*label {
	return b.bucket.labels
}

func (b *stateBucket) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           b.ID(),
		name:         b.Name(),
		pkgName:      b.PkgName(),
		resourceType: b.resourceType(),
		shouldRemove: b.shouldRemove,
	}
}

func (b *stateBucket) shouldApply() bool {
	return b.shouldRemove ||
		b.existing == nil ||
		b.Description != b.existing.Description ||
		b.Name() != b.existing.Name ||
		b.RetentionRules.RP() != b.existing.RetentionPeriod
}

type stateLabel struct {
	id, orgID    influxdb.ID
	shouldRemove bool

	existing *influxdb.Label

	*label
}

func (l *stateLabel) diffLabel() DiffLabel {
	diff := DiffLabel{
		DiffIdentifier: DiffIdentifier{
			ID:      SafeID(l.ID()),
			Remove:  l.shouldRemove,
			PkgName: l.PkgName(),
		},
		New: DiffLabelValues{
			Name:        l.Name(),
			Description: l.Description,
			Color:       l.Color,
		},
	}
	if e := l.existing; e != nil {
		diff.Old = &DiffLabelValues{
			Name:        e.Name,
			Description: e.Properties["description"],
			Color:       e.Properties["color"],
		}
	}
	return diff
}

func (l *stateLabel) summarize() SummaryLabel {
	sum := l.label.summarize()
	sum.ID = SafeID(l.ID())
	sum.OrgID = SafeID(l.orgID)
	return sum
}

func (l *stateLabel) Exists() bool {
	return l.existing != nil
}

func (l *stateLabel) ID() influxdb.ID {
	if l.Exists() {
		return l.existing.ID
	}
	return l.id
}

func (l *stateLabel) shouldApply() bool {
	return l.existing == nil ||
		l.Description != l.existing.Properties["description"] ||
		l.Name() != l.existing.Name ||
		l.Color != l.existing.Properties["color"]
}

func (l *stateLabel) toInfluxLabel() influxdb.Label {
	return influxdb.Label{
		ID:         l.ID(),
		OrgID:      l.orgID,
		Name:       l.Name(),
		Properties: l.properties(),
	}
}

func (l *stateLabel) properties() map[string]string {
	return map[string]string{
		"color":       l.Color,
		"description": l.Description,
	}
}

type stateLabelMapping struct {
	status StateStatus

	resource interface {
		stateIdentity() stateIdentity
	}

	label *stateLabel
}

func (lm stateLabelMapping) diffLabelMapping() DiffLabelMapping {
	ident := lm.resource.stateIdentity()
	return DiffLabelMapping{
		StateStatus:  lm.status,
		ResType:      ident.resourceType,
		ResID:        SafeID(ident.id),
		ResPkgName:   ident.pkgName,
		ResName:      ident.name,
		LabelID:      SafeID(lm.label.ID()),
		LabelPkgName: lm.label.PkgName(),
		LabelName:    lm.label.Name(),
	}
}

func (lm stateLabelMapping) summarize() SummaryLabelMapping {
	ident := lm.resource.stateIdentity()
	return SummaryLabelMapping{
		Status:          lm.status,
		ResourceID:      SafeID(ident.id),
		ResourcePkgName: ident.pkgName,
		ResourceName:    ident.name,
		ResourceType:    ident.resourceType,
		LabelPkgName:    lm.label.PkgName(),
		LabelName:       lm.label.Name(),
		LabelID:         SafeID(lm.label.ID()),
	}
}

func stateLabelMappingToInfluxLabelMapping(mapping stateLabelMapping) influxdb.LabelMapping {
	ident := mapping.resource.stateIdentity()
	return influxdb.LabelMapping{
		LabelID:      mapping.label.ID(),
		ResourceID:   ident.id,
		ResourceType: ident.resourceType,
	}
}

// IsNew identifies state status as new to the platform.
func IsNew(status StateStatus) bool {
	return status == StateStatusNew
}

func exists(status StateStatus) bool {
	return status == StateStatusExists
}
