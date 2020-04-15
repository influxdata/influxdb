package pkger

import (
	"reflect"
	"sort"

	"github.com/influxdata/influxdb/v2"
)

type stateCoordinator struct {
	mBuckets   map[string]*stateBucket
	mChecks    map[string]*stateCheck
	mEndpoints map[string]*stateEndpoint
	mLabels    map[string]*stateLabel
	mVariables map[string]*stateVariable

	labelMappings []stateLabelMapping
}

func newStateCoordinator(pkg *Pkg) *stateCoordinator {
	state := stateCoordinator{
		mBuckets:   make(map[string]*stateBucket),
		mChecks:    make(map[string]*stateCheck),
		mEndpoints: make(map[string]*stateEndpoint),
		mLabels:    make(map[string]*stateLabel),
		mVariables: make(map[string]*stateVariable),
	}

	for _, pkgBkt := range pkg.buckets() {
		state.mBuckets[pkgBkt.PkgName()] = &stateBucket{
			parserBkt:   pkgBkt,
			stateStatus: StateStatusNew,
		}
	}
	for _, pkgCheck := range pkg.checks() {
		state.mChecks[pkgCheck.PkgName()] = &stateCheck{
			parserCheck: pkgCheck,
			stateStatus: StateStatusNew,
		}
	}
	for _, pkgEndpoint := range pkg.notificationEndpoints() {
		state.mEndpoints[pkgEndpoint.PkgName()] = &stateEndpoint{
			parserEndpoint: pkgEndpoint,
			stateStatus:    StateStatusNew,
		}
	}
	for _, pkgLabel := range pkg.labels() {
		state.mLabels[pkgLabel.PkgName()] = &stateLabel{
			parserLabel: pkgLabel,
			stateStatus: StateStatusNew,
		}
	}
	for _, pkgVar := range pkg.variables() {
		state.mVariables[pkgVar.PkgName()] = &stateVariable{
			parserVar:   pkgVar,
			stateStatus: StateStatusNew,
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

func (s *stateCoordinator) checks() []*stateCheck {
	out := make([]*stateCheck, 0, len(s.mChecks))
	for _, v := range s.mChecks {
		out = append(out, v)
	}
	return out
}

func (s *stateCoordinator) endpoints() []*stateEndpoint {
	out := make([]*stateEndpoint, 0, len(s.mEndpoints))
	for _, e := range s.mEndpoints {
		out = append(out, e)
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

func (s *stateCoordinator) variables() []*stateVariable {
	out := make([]*stateVariable, 0, len(s.mVariables))
	for _, v := range s.mVariables {
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

	for _, c := range s.mChecks {
		diff.Checks = append(diff.Checks, c.diffCheck())
	}
	sort.Slice(diff.Checks, func(i, j int) bool {
		return diff.Checks[i].PkgName < diff.Checks[j].PkgName
	})

	for _, e := range s.mEndpoints {
		diff.NotificationEndpoints = append(diff.NotificationEndpoints, e.diffEndpoint())
	}
	sort.Slice(diff.NotificationEndpoints, func(i, j int) bool {
		return diff.NotificationEndpoints[i].PkgName < diff.NotificationEndpoints[j].PkgName
	})

	for _, l := range s.mLabels {
		diff.Labels = append(diff.Labels, l.diffLabel())
	}
	sort.Slice(diff.Labels, func(i, j int) bool {
		return diff.Labels[i].PkgName < diff.Labels[j].PkgName
	})

	for _, v := range s.mVariables {
		diff.Variables = append(diff.Variables, v.diffVariable())
	}
	sort.Slice(diff.Variables, func(i, j int) bool {
		return diff.Variables[i].PkgName < diff.Variables[j].PkgName
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
	for _, v := range s.mBuckets {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.Buckets = append(sum.Buckets, v.summarize())
	}
	sort.Slice(sum.Buckets, func(i, j int) bool {
		return sum.Buckets[i].PkgName < sum.Buckets[j].PkgName
	})

	for _, c := range s.mChecks {
		if IsRemoval(c.stateStatus) {
			continue
		}
		sum.Checks = append(sum.Checks, c.summarize())
	}
	sort.Slice(sum.Checks, func(i, j int) bool {
		return sum.Checks[i].PkgName < sum.Checks[j].PkgName
	})

	for _, e := range s.mEndpoints {
		if IsRemoval(e.stateStatus) {
			continue
		}
		sum.NotificationEndpoints = append(sum.NotificationEndpoints, e.summarize())
	}

	for _, v := range s.mLabels {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.Labels = append(sum.Labels, v.summarize())
	}
	sort.Slice(sum.Labels, func(i, j int) bool {
		return sum.Labels[i].PkgName < sum.Labels[j].PkgName
	})

	for _, v := range s.mVariables {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.Variables = append(sum.Variables, v.summarize())
	}
	sort.Slice(sum.Variables, func(i, j int) bool {
		return sum.Variables[i].PkgName < sum.Variables[j].PkgName
	})

	for _, v := range s.labelMappings {
		sum.LabelMappings = append(sum.LabelMappings, v.summarize())
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
			id:          id,
			parserBkt:   &bucket{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		s.mChecks[pkgName] = &stateCheck{
			id:          id,
			parserCheck: &check{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindLabel:
		s.mLabels[pkgName] = &stateLabel{
			id:          id,
			parserLabel: &label{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		s.mEndpoints[pkgName] = &stateEndpoint{
			id:             id,
			parserEndpoint: &notificationEndpoint{identity: newIdentity},
			stateStatus:    StateStatusRemove,
		}
	case KindVariable:
		s.mVariables[pkgName] = &stateVariable{
			id:          id,
			parserVar:   &variable{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	}
}

func (s *stateCoordinator) getObjectIDSetter(k Kind, pkgName string) (func(influxdb.ID), bool) {
	switch k {
	case KindBucket:
		r, ok := s.mBuckets[pkgName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		r, ok := s.mChecks[pkgName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindLabel:
		r, ok := s.mLabels[pkgName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		r, ok := s.mEndpoints[pkgName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindVariable:
		r, ok := s.mVariables[pkgName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
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
	stateStatus  StateStatus
}

func (s stateIdentity) exists() bool {
	return IsExisting(s.stateStatus)
}

type stateBucket struct {
	id, orgID   influxdb.ID
	stateStatus StateStatus

	parserBkt *bucket
	existing  *influxdb.Bucket
}

func (b *stateBucket) diffBucket() DiffBucket {
	diff := DiffBucket{
		DiffIdentifier: DiffIdentifier{
			ID:          SafeID(b.ID()),
			Remove:      IsRemoval(b.stateStatus),
			StateStatus: b.stateStatus,
			PkgName:     b.parserBkt.PkgName(),
		},
		New: DiffBucketValues{
			Name:           b.parserBkt.Name(),
			Description:    b.parserBkt.Description,
			RetentionRules: b.parserBkt.RetentionRules,
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
	sum := b.parserBkt.summarize()
	sum.ID = SafeID(b.ID())
	sum.OrgID = SafeID(b.orgID)
	return sum
}

func (b *stateBucket) ID() influxdb.ID {
	if !IsNew(b.stateStatus) && b.existing != nil {
		return b.existing.ID
	}
	return b.id
}

func (b *stateBucket) resourceType() influxdb.ResourceType {
	return KindBucket.ResourceType()
}

func (b *stateBucket) labels() []*label {
	return b.parserBkt.labels
}

func (b *stateBucket) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           b.ID(),
		name:         b.parserBkt.Name(),
		pkgName:      b.parserBkt.PkgName(),
		resourceType: b.resourceType(),
		stateStatus:  b.stateStatus,
	}
}

func (b *stateBucket) shouldApply() bool {
	return IsRemoval(b.stateStatus) ||
		b.existing == nil ||
		b.parserBkt.Description != b.existing.Description ||
		b.parserBkt.Name() != b.existing.Name ||
		b.parserBkt.RetentionRules.RP() != b.existing.RetentionPeriod
}

type stateCheck struct {
	id, orgID   influxdb.ID
	stateStatus StateStatus

	parserCheck *check
	existing    influxdb.Check
}

func (c *stateCheck) ID() influxdb.ID {
	if IsExisting(c.stateStatus) && c.existing != nil {
		return c.existing.GetID()
	}
	return c.id
}

func (c *stateCheck) labels() []*label {
	return c.parserCheck.labels
}

func (c *stateCheck) resourceType() influxdb.ResourceType {
	return KindCheck.ResourceType()
}

func (c *stateCheck) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           c.ID(),
		name:         c.parserCheck.Name(),
		pkgName:      c.parserCheck.PkgName(),
		resourceType: c.resourceType(),
		stateStatus:  c.stateStatus,
	}
}

func (c *stateCheck) diffCheck() DiffCheck {
	diff := DiffCheck{
		DiffIdentifier: DiffIdentifier{
			ID:          SafeID(c.ID()),
			Remove:      IsRemoval(c.stateStatus),
			StateStatus: c.stateStatus,
			PkgName:     c.parserCheck.PkgName(),
		},
	}
	if newCheck := c.summarize(); newCheck.Check != nil {
		diff.New.Check = newCheck.Check
	}
	if c.existing != nil {
		diff.Old = &DiffCheckValues{
			Check: c.existing,
		}
	}
	return diff
}

func (c *stateCheck) summarize() SummaryCheck {
	sum := c.parserCheck.summarize()
	if sum.Check == nil {
		return sum
	}
	sum.Check.SetID(c.id)
	sum.Check.SetOrgID(c.orgID)
	return sum
}

type stateLabel struct {
	id, orgID   influxdb.ID
	stateStatus StateStatus

	parserLabel *label
	existing    *influxdb.Label
}

func (l *stateLabel) diffLabel() DiffLabel {
	diff := DiffLabel{
		DiffIdentifier: DiffIdentifier{
			ID: SafeID(l.ID()),
			// TODO: axe Remove field when StateStatus is adopted
			Remove:      IsRemoval(l.stateStatus),
			StateStatus: l.stateStatus,
			PkgName:     l.parserLabel.PkgName(),
		},
		New: DiffLabelValues{
			Name:        l.parserLabel.Name(),
			Description: l.parserLabel.Description,
			Color:       l.parserLabel.Color,
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
	sum := l.parserLabel.summarize()
	sum.ID = SafeID(l.ID())
	sum.OrgID = SafeID(l.orgID)
	return sum
}

func (l *stateLabel) ID() influxdb.ID {
	if !IsNew(l.stateStatus) && l.existing != nil {
		return l.existing.ID
	}
	return l.id
}

func (l *stateLabel) shouldApply() bool {
	return IsRemoval(l.stateStatus) ||
		l.existing == nil ||
		l.parserLabel.Description != l.existing.Properties["description"] ||
		l.parserLabel.Name() != l.existing.Name ||
		l.parserLabel.Color != l.existing.Properties["color"]
}

func (l *stateLabel) toInfluxLabel() influxdb.Label {
	return influxdb.Label{
		ID:         l.ID(),
		OrgID:      l.orgID,
		Name:       l.parserLabel.Name(),
		Properties: l.properties(),
	}
}

func (l *stateLabel) properties() map[string]string {
	return map[string]string{
		"color":       l.parserLabel.Color,
		"description": l.parserLabel.Description,
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
		LabelPkgName: lm.label.parserLabel.PkgName(),
		LabelName:    lm.label.parserLabel.Name(),
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
		LabelPkgName:    lm.label.parserLabel.PkgName(),
		LabelName:       lm.label.parserLabel.Name(),
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

type stateEndpoint struct {
	id, orgID   influxdb.ID
	stateStatus StateStatus

	parserEndpoint *notificationEndpoint
	existing       influxdb.NotificationEndpoint
}

func (e *stateEndpoint) ID() influxdb.ID {
	if !IsNew(e.stateStatus) && e.existing != nil {
		return e.existing.GetID()
	}
	return e.id
}

func (e *stateEndpoint) diffEndpoint() DiffNotificationEndpoint {
	diff := DiffNotificationEndpoint{
		DiffIdentifier: DiffIdentifier{
			ID:          SafeID(e.ID()),
			Remove:      IsRemoval(e.stateStatus),
			StateStatus: e.stateStatus,
			PkgName:     e.parserEndpoint.PkgName(),
		},
	}
	if sum := e.summarize(); sum.NotificationEndpoint != nil {
		diff.New.NotificationEndpoint = sum.NotificationEndpoint
	}
	if e.existing != nil {
		diff.Old = &DiffNotificationEndpointValues{
			NotificationEndpoint: e.existing,
		}
	}
	return diff
}

func (e *stateEndpoint) labels() []*label {
	return e.parserEndpoint.labels
}

func (e *stateEndpoint) resourceType() influxdb.ResourceType {
	return KindNotificationEndpoint.ResourceType()
}

func (e *stateEndpoint) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           e.ID(),
		name:         e.parserEndpoint.Name(),
		pkgName:      e.parserEndpoint.PkgName(),
		resourceType: e.resourceType(),
		stateStatus:  e.stateStatus,
	}
}

func (e *stateEndpoint) summarize() SummaryNotificationEndpoint {
	sum := e.parserEndpoint.summarize()
	if sum.NotificationEndpoint == nil {
		return sum
	}
	if e.ID() != 0 {
		sum.NotificationEndpoint.SetID(e.ID())
	}
	if e.orgID != 0 {
		sum.NotificationEndpoint.SetOrgID(e.orgID)
	}
	return sum
}

type stateVariable struct {
	id, orgID   influxdb.ID
	stateStatus StateStatus

	parserVar *variable
	existing  *influxdb.Variable
}

func (v *stateVariable) ID() influxdb.ID {
	if !IsNew(v.stateStatus) && v.existing != nil {
		return v.existing.ID
	}
	return v.id
}

func (v *stateVariable) diffVariable() DiffVariable {
	diff := DiffVariable{
		DiffIdentifier: DiffIdentifier{
			ID:          SafeID(v.ID()),
			Remove:      IsRemoval(v.stateStatus),
			StateStatus: v.stateStatus,
			PkgName:     v.parserVar.PkgName(),
		},
		New: DiffVariableValues{
			Name:        v.parserVar.Name(),
			Description: v.parserVar.Description,
			Args:        v.parserVar.influxVarArgs(),
		},
	}
	if iv := v.existing; iv != nil {
		diff.Old = &DiffVariableValues{
			Name:        iv.Name,
			Description: iv.Description,
			Args:        iv.Arguments,
		}
	}

	return diff
}

func (v *stateVariable) labels() []*label {
	return v.parserVar.labels
}

func (v *stateVariable) resourceType() influxdb.ResourceType {
	return KindVariable.ResourceType()
}

func (v *stateVariable) shouldApply() bool {
	return IsRemoval(v.stateStatus) ||
		v.existing == nil ||
		v.existing.Description != v.parserVar.Description ||
		v.existing.Arguments == nil ||
		!reflect.DeepEqual(v.existing.Arguments, v.parserVar.influxVarArgs())
}

func (v *stateVariable) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           v.ID(),
		name:         v.parserVar.Name(),
		pkgName:      v.parserVar.PkgName(),
		resourceType: v.resourceType(),
		stateStatus:  v.stateStatus,
	}
}

func (v *stateVariable) summarize() SummaryVariable {
	sum := v.parserVar.summarize()
	sum.ID = SafeID(v.ID())
	sum.OrgID = SafeID(v.orgID)
	return sum
}

// IsNew identifies state status as new to the platform.
func IsNew(status StateStatus) bool {
	// defaulting zero value to identify as new
	return status == StateStatusNew || status == ""
}

// IsExisting identifies state status as existing in the platform.
func IsExisting(status StateStatus) bool {
	return status == StateStatusExists
}

// IsRemoval identifies state status as existing resource that will be removed
// from the platform.
func IsRemoval(status StateStatus) bool {
	return status == StateStatusRemove
}
