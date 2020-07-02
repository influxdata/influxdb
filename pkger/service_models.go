package pkger

import (
	"reflect"
	"sort"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

type stateCoordinator struct {
	stackID influxdb.ID

	mBuckets    map[string]*stateBucket
	mChecks     map[string]*stateCheck
	mDashboards map[string]*stateDashboard
	mEndpoints  map[string]*stateEndpoint
	mLabels     map[string]*stateLabel
	mRules      map[string]*stateRule
	mTasks      map[string]*stateTask
	mTelegrafs  map[string]*stateTelegraf
	mVariables  map[string]*stateVariable

	labelMappings         []stateLabelMapping
	labelMappingsToRemove []stateLabelMappingForRemoval
}

func newStateCoordinator(stackID influxdb.ID, template *Template, acts resourceActions) *stateCoordinator {
	state := stateCoordinator{
		stackID:     stackID,
		mBuckets:    make(map[string]*stateBucket),
		mChecks:     make(map[string]*stateCheck),
		mDashboards: make(map[string]*stateDashboard),
		mEndpoints:  make(map[string]*stateEndpoint),
		mLabels:     make(map[string]*stateLabel),
		mRules:      make(map[string]*stateRule),
		mTasks:      make(map[string]*stateTask),
		mTelegrafs:  make(map[string]*stateTelegraf),
		mVariables:  make(map[string]*stateVariable),
	}

	// labels are done first to validate dependencies are accounted for.
	// when a label is skipped by an action, this will still be accurate
	// for hte individual labels, and cascades to the resources that are
	// associated to a label.
	for _, l := range template.labels() {
		if acts.skipResource(KindLabel, l.MetaName()) {
			continue
		}
		state.mLabels[l.MetaName()] = &stateLabel{
			parserLabel: l,
			stateStatus: StateStatusNew,
		}
	}
	for _, b := range template.buckets() {
		if acts.skipResource(KindBucket, b.MetaName()) {
			continue
		}
		state.mBuckets[b.MetaName()] = &stateBucket{
			parserBkt:         b,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(b.labels),
		}
	}
	for _, c := range template.checks() {
		if acts.skipResource(KindCheck, c.MetaName()) {
			continue
		}
		state.mChecks[c.MetaName()] = &stateCheck{
			parserCheck:       c,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(c.labels),
		}
	}
	for _, d := range template.dashboards() {
		if acts.skipResource(KindDashboard, d.MetaName()) {
			continue
		}
		state.mDashboards[d.MetaName()] = &stateDashboard{
			parserDash:        d,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(d.labels),
		}
	}
	for _, e := range template.notificationEndpoints() {
		if acts.skipResource(KindNotificationEndpoint, e.MetaName()) {
			continue
		}
		state.mEndpoints[e.MetaName()] = &stateEndpoint{
			parserEndpoint:    e,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(e.labels),
		}
	}
	for _, r := range template.notificationRules() {
		if acts.skipResource(KindNotificationRule, r.MetaName()) {
			continue
		}
		state.mRules[r.MetaName()] = &stateRule{
			parserRule:        r,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(r.labels),
		}
	}
	for _, task := range template.tasks() {
		if acts.skipResource(KindTask, task.MetaName()) {
			continue
		}
		state.mTasks[task.MetaName()] = &stateTask{
			parserTask:        task,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(task.labels),
		}
	}
	for _, tele := range template.telegrafs() {
		if acts.skipResource(KindTelegraf, tele.MetaName()) {
			continue
		}
		state.mTelegrafs[tele.MetaName()] = &stateTelegraf{
			parserTelegraf:    tele,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(tele.labels),
		}
	}
	for _, v := range template.variables() {
		if acts.skipResource(KindVariable, v.MetaName()) {
			continue
		}
		state.mVariables[v.MetaName()] = &stateVariable{
			parserVar:         v,
			stateStatus:       StateStatusNew,
			labelAssociations: state.templateToStateLabels(v.labels),
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

func (s *stateCoordinator) dashboards() []*stateDashboard {
	out := make([]*stateDashboard, 0, len(s.mDashboards))
	for _, d := range s.mDashboards {
		out = append(out, d)
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

func (s *stateCoordinator) rules() []*stateRule {
	out := make([]*stateRule, 0, len(s.mRules))
	for _, r := range s.mRules {
		out = append(out, r)
	}
	return out
}

func (s *stateCoordinator) tasks() []*stateTask {
	out := make([]*stateTask, 0, len(s.mTasks))
	for _, t := range s.mTasks {
		out = append(out, t)
	}
	return out
}

func (s *stateCoordinator) telegrafConfigs() []*stateTelegraf {
	out := make([]*stateTelegraf, 0, len(s.mTelegrafs))
	for _, t := range s.mTelegrafs {
		out = append(out, t)
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
		return diff.Buckets[i].MetaName < diff.Buckets[j].MetaName
	})

	for _, c := range s.mChecks {
		diff.Checks = append(diff.Checks, c.diffCheck())
	}
	sort.Slice(diff.Checks, func(i, j int) bool {
		return diff.Checks[i].MetaName < diff.Checks[j].MetaName
	})

	for _, d := range s.mDashboards {
		diff.Dashboards = append(diff.Dashboards, d.diffDashboard())
	}
	sort.Slice(diff.Dashboards, func(i, j int) bool {
		return diff.Dashboards[i].MetaName < diff.Dashboards[j].MetaName
	})

	for _, e := range s.mEndpoints {
		diff.NotificationEndpoints = append(diff.NotificationEndpoints, e.diffEndpoint())
	}
	sort.Slice(diff.NotificationEndpoints, func(i, j int) bool {
		return diff.NotificationEndpoints[i].MetaName < diff.NotificationEndpoints[j].MetaName
	})

	for _, l := range s.mLabels {
		diff.Labels = append(diff.Labels, l.diffLabel())
	}
	sort.Slice(diff.Labels, func(i, j int) bool {
		return diff.Labels[i].MetaName < diff.Labels[j].MetaName
	})

	for _, r := range s.mRules {
		diff.NotificationRules = append(diff.NotificationRules, r.diffRule())
	}
	sort.Slice(diff.NotificationRules, func(i, j int) bool {
		return diff.NotificationRules[i].MetaName < diff.NotificationRules[j].MetaName
	})

	for _, t := range s.mTasks {
		diff.Tasks = append(diff.Tasks, t.diffTask())
	}
	sort.Slice(diff.Tasks, func(i, j int) bool {
		return diff.Tasks[i].MetaName < diff.Tasks[j].MetaName
	})

	for _, t := range s.mTelegrafs {
		diff.Telegrafs = append(diff.Telegrafs, t.diffTelegraf())
	}
	sort.Slice(diff.Telegrafs, func(i, j int) bool {
		return diff.Telegrafs[i].MetaName < diff.Telegrafs[j].MetaName
	})

	for _, v := range s.mVariables {
		diff.Variables = append(diff.Variables, v.diffVariable())
	}
	sort.Slice(diff.Variables, func(i, j int) bool {
		return diff.Variables[i].MetaName < diff.Variables[j].MetaName
	})

	for _, m := range s.labelMappings {
		diff.LabelMappings = append(diff.LabelMappings, m.diffLabelMapping())
	}
	for _, m := range s.labelMappingsToRemove {
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
		if n.ResMetaName < m.ResMetaName {
			return true
		}
		if n.ResMetaName > m.ResMetaName {
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
		return sum.Buckets[i].MetaName < sum.Buckets[j].MetaName
	})

	for _, c := range s.mChecks {
		if IsRemoval(c.stateStatus) {
			continue
		}
		sum.Checks = append(sum.Checks, c.summarize())
	}
	sort.Slice(sum.Checks, func(i, j int) bool {
		return sum.Checks[i].MetaName < sum.Checks[j].MetaName
	})

	for _, d := range s.mDashboards {
		if IsRemoval(d.stateStatus) {
			continue
		}
		sum.Dashboards = append(sum.Dashboards, d.summarize())
	}
	sort.Slice(sum.Dashboards, func(i, j int) bool {
		return sum.Dashboards[i].MetaName < sum.Dashboards[j].MetaName
	})

	for _, e := range s.mEndpoints {
		if IsRemoval(e.stateStatus) {
			continue
		}
		sum.NotificationEndpoints = append(sum.NotificationEndpoints, e.summarize())
	}
	sort.Slice(sum.NotificationEndpoints, func(i, j int) bool {
		return sum.NotificationEndpoints[i].MetaName < sum.NotificationEndpoints[j].MetaName
	})

	for _, v := range s.mLabels {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.Labels = append(sum.Labels, v.summarize())
	}
	sort.Slice(sum.Labels, func(i, j int) bool {
		return sum.Labels[i].MetaName < sum.Labels[j].MetaName
	})

	for _, v := range s.mRules {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.NotificationRules = append(sum.NotificationRules, v.summarize())
	}
	sort.Slice(sum.NotificationRules, func(i, j int) bool {
		return sum.NotificationRules[i].MetaName < sum.NotificationRules[j].MetaName
	})

	for _, t := range s.mTasks {
		if IsRemoval(t.stateStatus) {
			continue
		}
		sum.Tasks = append(sum.Tasks, t.summarize())
	}
	sort.Slice(sum.Tasks, func(i, j int) bool {
		return sum.Tasks[i].MetaName < sum.Tasks[j].MetaName
	})

	for _, t := range s.mTelegrafs {
		if IsRemoval(t.stateStatus) {
			continue
		}
		sum.TelegrafConfigs = append(sum.TelegrafConfigs, t.summarize())
	}
	sort.Slice(sum.TelegrafConfigs, func(i, j int) bool {
		return sum.TelegrafConfigs[i].MetaName < sum.TelegrafConfigs[j].MetaName
	})

	for _, v := range s.mVariables {
		if IsRemoval(v.stateStatus) {
			continue
		}
		sum.Variables = append(sum.Variables, v.summarize())
	}
	sort.Slice(sum.Variables, func(i, j int) bool {
		return sum.Variables[i].MetaName < sum.Variables[j].MetaName
	})

	for _, v := range s.labelMappings {
		sum.LabelMappings = append(sum.LabelMappings, v.summarize())
	}
	sort.Slice(sum.LabelMappings, func(i, j int) bool {
		n, m := sum.LabelMappings[i], sum.LabelMappings[j]
		if n.ResourceType != m.ResourceType {
			return n.ResourceType < m.ResourceType
		}
		if n.ResourceMetaName != m.ResourceMetaName {
			return n.ResourceMetaName < m.ResourceMetaName
		}
		return n.LabelName < m.LabelName
	})

	return sum
}

func (s *stateCoordinator) getLabelByMetaName(metaName string) (*stateLabel, bool) {
	l, ok := s.mLabels[metaName]
	return l, ok
}

func (s *stateCoordinator) templateToStateLabels(labels []*label) []*stateLabel {
	var out []*stateLabel
	for _, l := range labels {
		stLabel, found := s.getLabelByMetaName(l.MetaName())
		if !found {
			continue
		}
		out = append(out, stLabel)
	}
	return out
}

func (s *stateCoordinator) addStackState(stack Stack) {
	reconcilers := []func([]StackResource){
		s.reconcileStackResources,
		s.reconcileLabelMappings,
		s.reconcileNotificationDependencies,
	}
	for _, reconcileFn := range reconcilers {
		reconcileFn(stack.Resources)
	}
}

func (s *stateCoordinator) reconcileStackResources(stackResources []StackResource) {
	for _, r := range stackResources {
		if !s.Contains(r.Kind, r.MetaName) {
			s.addObjectForRemoval(r.Kind, r.MetaName, r.ID)
			continue
		}
		s.setObjectID(r.Kind, r.MetaName, r.ID)
	}
}

func (s *stateCoordinator) reconcileLabelMappings(stackResources []StackResource) {
	mLabelMetaNameToID := make(map[string]influxdb.ID)
	for _, r := range stackResources {
		if r.Kind.is(KindLabel) {
			mLabelMetaNameToID[r.MetaName] = r.ID
		}
	}

	for _, r := range stackResources {
		labels := s.labelAssociations(r.Kind, r.MetaName)
		if len(r.Associations) == 0 {
			continue
		}

		// if associations agree => do nothing
		// if associations are new (in state not in stack) => do nothing
		// if associations are not in state and in stack => add them for removal
		mStackAss := make(map[StackResourceAssociation]struct{})
		for _, ass := range r.Associations {
			if ass.Kind.is(KindLabel) {
				mStackAss[ass] = struct{}{}
			}
		}

		for _, l := range labels {
			// we want to keep associations that are from previous application and are not changing
			delete(mStackAss, StackResourceAssociation{
				Kind:     KindLabel,
				MetaName: l.parserLabel.MetaName(),
			})
		}

		// all associations that are in the stack but not in the
		// state fall into here and are marked for removal.
		for assForRemoval := range mStackAss {
			s.labelMappingsToRemove = append(s.labelMappingsToRemove, stateLabelMappingForRemoval{
				LabelMetaName:    assForRemoval.MetaName,
				LabelID:          mLabelMetaNameToID[assForRemoval.MetaName],
				ResourceID:       r.ID,
				ResourceMetaName: r.MetaName,
				ResourceType:     r.Kind.ResourceType(),
			})
		}
	}
}

func (s *stateCoordinator) reconcileNotificationDependencies(stackResources []StackResource) {
	for _, r := range stackResources {
		if r.Kind.is(KindNotificationRule) {
			for _, ass := range r.Associations {
				if ass.Kind.is(KindNotificationEndpoint) {
					s.mRules[r.MetaName].associatedEndpoint = s.mEndpoints[ass.MetaName]
					break
				}
			}
		}
	}
}

func (s *stateCoordinator) get(k Kind, metaName string) (interface{}, bool) {
	switch k {
	case KindBucket:
		v, ok := s.mBuckets[metaName]
		return v, ok
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		v, ok := s.mChecks[metaName]
		return v, ok
	case KindDashboard:
		v, ok := s.mDashboards[metaName]
		return v, ok
	case KindLabel:
		v, ok := s.mLabels[metaName]
		return v, ok
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		v, ok := s.mEndpoints[metaName]
		return v, ok
	case KindNotificationRule:
		v, ok := s.mRules[metaName]
		return v, ok
	case KindTask:
		v, ok := s.mTasks[metaName]
		return v, ok
	case KindTelegraf:
		v, ok := s.mTelegrafs[metaName]
		return v, ok
	case KindVariable:
		v, ok := s.mVariables[metaName]
		return v, ok
	default:
		return nil, false
	}
}

func (s *stateCoordinator) labelAssociations(k Kind, metaName string) []*stateLabel {
	v, _ := s.get(k, metaName)
	labeler, ok := v.(interface {
		labels() []*stateLabel
	})
	if !ok {
		return nil
	}

	return labeler.labels()
}

func (s *stateCoordinator) Contains(k Kind, metaName string) bool {
	_, ok := s.get(k, metaName)
	return ok
}

// setObjectID sets the id for the resource graphed from the object the key identifies.
func (s *stateCoordinator) setObjectID(k Kind, metaName string, id influxdb.ID) {
	idSetFn, ok := s.getObjectIDSetter(k, metaName)
	if !ok {
		return
	}
	idSetFn(id)
}

// addObjectForRemoval sets the id for the resource graphed from the object the key identifies.
// The metaName and kind are used as the unique identifier, when calling this it will
// overwrite any existing value if one exists. If desired, check for the value by using
// the Contains method.
func (s *stateCoordinator) addObjectForRemoval(k Kind, metaName string, id influxdb.ID) {
	newIdentity := identity{
		name: &references{val: metaName},
	}

	switch k {
	case KindBucket:
		s.mBuckets[metaName] = &stateBucket{
			id:          id,
			parserBkt:   &bucket{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		s.mChecks[metaName] = &stateCheck{
			id:          id,
			parserCheck: &check{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindDashboard:
		s.mDashboards[metaName] = &stateDashboard{
			id:          id,
			parserDash:  &dashboard{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindLabel:
		s.mLabels[metaName] = &stateLabel{
			id:          id,
			parserLabel: &label{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		s.mEndpoints[metaName] = &stateEndpoint{
			id:             id,
			parserEndpoint: &notificationEndpoint{identity: newIdentity},
			stateStatus:    StateStatusRemove,
		}
	case KindNotificationRule:
		s.mRules[metaName] = &stateRule{
			id:          id,
			parserRule:  &notificationRule{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindTask:
		s.mTasks[metaName] = &stateTask{
			id:          id,
			parserTask:  &task{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	case KindTelegraf:
		s.mTelegrafs[metaName] = &stateTelegraf{
			id:             id,
			parserTelegraf: &telegraf{identity: newIdentity},
			stateStatus:    StateStatusRemove,
		}
	case KindVariable:
		s.mVariables[metaName] = &stateVariable{
			id:          id,
			parserVar:   &variable{identity: newIdentity},
			stateStatus: StateStatusRemove,
		}
	}
}

func (s *stateCoordinator) getObjectIDSetter(k Kind, metaName string) (func(influxdb.ID), bool) {
	switch k {
	case KindBucket:
		r, ok := s.mBuckets[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		r, ok := s.mChecks[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindDashboard:
		r, ok := s.mDashboards[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindLabel:
		r, ok := s.mLabels[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		r, ok := s.mEndpoints[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindNotificationRule:
		r, ok := s.mRules[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindTask:
		r, ok := s.mTasks[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindTelegraf:
		r, ok := s.mTelegrafs[metaName]
		return func(id influxdb.ID) {
			r.id = id
			r.stateStatus = StateStatusExists
		}, ok
	case KindVariable:
		r, ok := s.mVariables[metaName]
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
	metaName     string
	resourceType influxdb.ResourceType
	stateStatus  StateStatus
}

func (s stateIdentity) exists() bool {
	return IsExisting(s.stateStatus)
}

type stateBucket struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	parserBkt *bucket
	existing  *influxdb.Bucket
}

func (b *stateBucket) diffBucket() DiffBucket {
	diff := DiffBucket{
		DiffIdentifier: DiffIdentifier{
			Kind:        KindBucket,
			ID:          SafeID(b.ID()),
			StateStatus: b.stateStatus,
			MetaName:    b.parserBkt.MetaName(),
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

func stateToSummaryLabels(labels []*stateLabel) []SummaryLabel {
	out := make([]SummaryLabel, 0, len(labels))
	for _, l := range labels {
		out = append(out, l.summarize())
	}
	return out
}

func (b *stateBucket) summarize() SummaryBucket {
	sum := b.parserBkt.summarize()
	sum.ID = SafeID(b.ID())
	sum.OrgID = SafeID(b.orgID)
	sum.LabelAssociations = stateToSummaryLabels(b.labelAssociations)
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

func (b *stateBucket) labels() []*stateLabel {
	return b.labelAssociations
}

func (b *stateBucket) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           b.ID(),
		name:         b.parserBkt.Name(),
		metaName:     b.parserBkt.MetaName(),
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
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	parserCheck *check
	existing    influxdb.Check
}

func (c *stateCheck) ID() influxdb.ID {
	if !IsNew(c.stateStatus) && c.existing != nil {
		return c.existing.GetID()
	}
	return c.id
}

func (c *stateCheck) labels() []*stateLabel {
	return c.labelAssociations
}

func (c *stateCheck) resourceType() influxdb.ResourceType {
	return KindCheck.ResourceType()
}

func (c *stateCheck) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           c.ID(),
		name:         c.parserCheck.Name(),
		metaName:     c.parserCheck.MetaName(),
		resourceType: c.resourceType(),
		stateStatus:  c.stateStatus,
	}
}

func (c *stateCheck) diffCheck() DiffCheck {
	diff := DiffCheck{
		DiffIdentifier: DiffIdentifier{
			ID:          SafeID(c.ID()),
			StateStatus: c.stateStatus,
			MetaName:    c.parserCheck.MetaName(),
		},
	}
	newCheck := c.summarize()
	diff.Kind = newCheck.Kind
	if newCheck.Check != nil {
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
	sum.LabelAssociations = stateToSummaryLabels(c.labelAssociations)
	return sum
}

type stateDashboard struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	parserDash *dashboard
	existing   *influxdb.Dashboard
}

func (d *stateDashboard) ID() influxdb.ID {
	if !IsNew(d.stateStatus) && d.existing != nil {
		return d.existing.ID
	}
	return d.id
}

func (d *stateDashboard) labels() []*stateLabel {
	return d.labelAssociations
}

func (d *stateDashboard) resourceType() influxdb.ResourceType {
	return KindDashboard.ResourceType()
}

func (d *stateDashboard) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           d.ID(),
		name:         d.parserDash.Name(),
		metaName:     d.parserDash.MetaName(),
		resourceType: d.resourceType(),
		stateStatus:  d.stateStatus,
	}
}

func (d *stateDashboard) diffDashboard() DiffDashboard {
	diff := DiffDashboard{
		DiffIdentifier: DiffIdentifier{
			Kind:        KindDashboard,
			ID:          SafeID(d.ID()),
			StateStatus: d.stateStatus,
			MetaName:    d.parserDash.MetaName(),
		},
		New: DiffDashboardValues{
			Name:   d.parserDash.Name(),
			Desc:   d.parserDash.Description,
			Charts: make([]DiffChart, 0, len(d.parserDash.Charts)),
		},
	}

	for _, c := range d.parserDash.Charts {
		diff.New.Charts = append(diff.New.Charts, DiffChart{
			Properties: c.properties(),
			Height:     c.Height,
			Width:      c.Width,
		})
	}

	if d.existing == nil {
		return diff
	}

	oldDiff := DiffDashboardValues{
		Name:   d.existing.Name,
		Desc:   d.existing.Description,
		Charts: make([]DiffChart, 0, len(d.existing.Cells)),
	}

	for _, c := range d.existing.Cells {
		var props influxdb.ViewProperties
		if c.View != nil {
			props = c.View.Properties
		}

		oldDiff.Charts = append(oldDiff.Charts, DiffChart{
			Properties: props,
			XPosition:  int(c.X),
			YPosition:  int(c.Y),
			Height:     int(c.H),
			Width:      int(c.W),
		})
	}

	diff.Old = &oldDiff

	return diff
}

func (d *stateDashboard) summarize() SummaryDashboard {
	sum := d.parserDash.summarize()
	sum.ID = SafeID(d.ID())
	sum.OrgID = SafeID(d.orgID)
	sum.LabelAssociations = stateToSummaryLabels(d.labelAssociations)
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
			Kind:        KindLabel,
			ID:          SafeID(l.ID()),
			StateStatus: l.stateStatus,
			MetaName:    l.parserLabel.MetaName(),
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

func (l *stateLabel) Name() string {
	return l.parserLabel.Name()
}

func (l *stateLabel) MetaName() string {
	return l.parserLabel.MetaName()
}

func (l *stateLabel) shouldApply() bool {
	return IsRemoval(l.stateStatus) ||
		l.existing == nil ||
		l.parserLabel.Description != l.existing.Properties["description"] ||
		l.parserLabel.Name() != l.existing.Name ||
		l.parserLabel.Color != l.existing.Properties["color"]
}

func (l *stateLabel) isOwnedByStackID(templateStackID influxdb.ID) bool {
	return l.existing == nil || isOwner(templateStackID, l.existing.Annotations.Stacks().Owner)
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
		StateStatus:   lm.status,
		ResType:       ident.resourceType,
		ResID:         SafeID(ident.id),
		ResMetaName:   ident.metaName,
		ResName:       ident.name,
		LabelID:       SafeID(lm.label.ID()),
		LabelMetaName: lm.label.parserLabel.MetaName(),
		LabelName:     lm.label.parserLabel.Name(),
	}
}

func (lm stateLabelMapping) summarize() SummaryLabelMapping {
	ident := lm.resource.stateIdentity()
	return SummaryLabelMapping{
		Status:           lm.status,
		ResourceID:       SafeID(ident.id),
		ResourceMetaName: ident.metaName,
		ResourceName:     ident.name,
		ResourceType:     ident.resourceType,
		LabelMetaName:    lm.label.parserLabel.MetaName(),
		LabelName:        lm.label.parserLabel.Name(),
		LabelID:          SafeID(lm.label.ID()),
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

type stateLabelMappingForRemoval struct {
	LabelID          influxdb.ID
	LabelMetaName    string
	ResourceID       influxdb.ID
	ResourceMetaName string
	ResourceType     influxdb.ResourceType
}

func (m *stateLabelMappingForRemoval) diffLabelMapping() DiffLabelMapping {
	return DiffLabelMapping{
		StateStatus:   StateStatusRemove,
		ResType:       m.ResourceType,
		ResID:         SafeID(m.ResourceID),
		ResMetaName:   m.ResourceMetaName,
		LabelID:       SafeID(m.LabelID),
		LabelMetaName: m.LabelMetaName,
	}
}

type stateEndpoint struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

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
			StateStatus: e.stateStatus,
			MetaName:    e.parserEndpoint.MetaName(),
		},
	}
	sum := e.summarize()
	diff.Kind = sum.Kind
	if sum.NotificationEndpoint != nil {
		diff.New.NotificationEndpoint = sum.NotificationEndpoint
	}
	if e.existing != nil {
		diff.Old = &DiffNotificationEndpointValues{
			NotificationEndpoint: e.existing,
		}
	}
	return diff
}

func (e *stateEndpoint) labels() []*stateLabel {
	return e.labelAssociations
}

func (e *stateEndpoint) resourceType() influxdb.ResourceType {
	return KindNotificationEndpoint.ResourceType()
}

func (e *stateEndpoint) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           e.ID(),
		name:         e.parserEndpoint.Name(),
		metaName:     e.parserEndpoint.MetaName(),
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
	sum.LabelAssociations = stateToSummaryLabels(e.labelAssociations)
	return sum
}

type stateRule struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	associatedEndpoint *stateEndpoint

	parserRule *notificationRule
	existing   influxdb.NotificationRule
}

func (r *stateRule) ID() influxdb.ID {
	if !IsNew(r.stateStatus) && r.existing != nil {
		return r.existing.GetID()
	}
	return r.id
}

func (r *stateRule) endpointAssociation() StackResourceAssociation {
	if r.associatedEndpoint == nil {
		return StackResourceAssociation{}
	}
	return StackResourceAssociation{
		Kind:     KindNotificationEndpoint,
		MetaName: r.endpointTemplateName(),
	}
}

func (r *stateRule) diffRule() DiffNotificationRule {
	sum := DiffNotificationRule{
		DiffIdentifier: DiffIdentifier{
			Kind:        KindNotificationRule,
			ID:          SafeID(r.ID()),
			StateStatus: r.stateStatus,
			MetaName:    r.parserRule.MetaName(),
		},
		New: DiffNotificationRuleValues{
			Name:            r.parserRule.Name(),
			Description:     r.parserRule.description,
			EndpointName:    r.endpointTemplateName(),
			EndpointID:      SafeID(r.endpointID()),
			EndpointType:    r.endpointType(),
			Every:           r.parserRule.every.String(),
			Offset:          r.parserRule.offset.String(),
			MessageTemplate: r.parserRule.msgTemplate,
			StatusRules:     toSummaryStatusRules(r.parserRule.statusRules),
			TagRules:        toSummaryTagRules(r.parserRule.tagRules),
		},
	}

	if r.existing == nil {
		return sum
	}

	sum.Old = &DiffNotificationRuleValues{
		Name:         r.existing.GetName(),
		Description:  r.existing.GetDescription(),
		EndpointName: r.existing.GetName(),
		EndpointID:   SafeID(r.existing.GetEndpointID()),
		EndpointType: r.existing.Type(),
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

	switch p := r.existing.(type) {
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

func (r *stateRule) endpointID() influxdb.ID {
	if r.associatedEndpoint != nil {
		return r.associatedEndpoint.ID()
	}
	return 0
}

func (r *stateRule) endpointTemplateName() string {
	if r.associatedEndpoint != nil && r.associatedEndpoint.parserEndpoint != nil {
		return r.associatedEndpoint.parserEndpoint.MetaName()
	}
	return ""
}

func (r *stateRule) endpointType() string {
	if r.associatedEndpoint != nil {
		return r.associatedEndpoint.parserEndpoint.kind.String()
	}
	return ""
}

func (r *stateRule) labels() []*stateLabel {
	return r.labelAssociations
}

func (r *stateRule) resourceType() influxdb.ResourceType {
	return KindNotificationRule.ResourceType()
}

func (r *stateRule) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           r.ID(),
		name:         r.parserRule.Name(),
		metaName:     r.parserRule.MetaName(),
		resourceType: r.resourceType(),
		stateStatus:  r.stateStatus,
	}
}

func (r *stateRule) summarize() SummaryNotificationRule {
	sum := r.parserRule.summarize()
	sum.ID = SafeID(r.id)
	sum.EndpointID = SafeID(r.associatedEndpoint.ID())
	sum.EndpointMetaName = r.associatedEndpoint.parserEndpoint.MetaName()
	sum.EndpointType = r.associatedEndpoint.parserEndpoint.kind.String()
	sum.LabelAssociations = stateToSummaryLabels(r.labelAssociations)
	return sum
}

func (r *stateRule) toInfluxRule() influxdb.NotificationRule {
	influxRule := r.parserRule.toInfluxRule()
	if r.ID() > 0 {
		influxRule.SetID(r.ID())
	}
	if r.orgID > 0 {
		influxRule.SetOrgID(r.orgID)
	}
	switch e := influxRule.(type) {
	case *rule.HTTP:
		e.EndpointID = r.associatedEndpoint.ID()
	case *rule.PagerDuty:
		e.EndpointID = r.associatedEndpoint.ID()
	case *rule.Slack:
		e.EndpointID = r.associatedEndpoint.ID()
	}

	return influxRule
}

type stateTask struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	parserTask *task
	existing   *influxdb.Task
}

func (t *stateTask) ID() influxdb.ID {
	if !IsNew(t.stateStatus) && t.existing != nil {
		return t.existing.ID
	}
	return t.id
}

func (t *stateTask) diffTask() DiffTask {
	diff := DiffTask{
		DiffIdentifier: DiffIdentifier{
			Kind:        KindTask,
			ID:          SafeID(t.ID()),
			StateStatus: t.stateStatus,
			MetaName:    t.parserTask.MetaName(),
		},
		New: DiffTaskValues{
			Name:        t.parserTask.Name(),
			Cron:        t.parserTask.cron,
			Description: t.parserTask.description,
			Every:       durToStr(t.parserTask.every),
			Offset:      durToStr(t.parserTask.offset),
			Query:       t.parserTask.query,
			Status:      t.parserTask.Status(),
		},
	}

	if t.existing == nil {
		return diff
	}

	diff.Old = &DiffTaskValues{
		Name:        t.existing.Name,
		Cron:        t.existing.Cron,
		Description: t.existing.Description,
		Every:       t.existing.Every,
		Offset:      t.existing.Offset.String(),
		Query:       t.existing.Flux,
		Status:      influxdb.Status(t.existing.Status),
	}

	return diff
}

func (t *stateTask) labels() []*stateLabel {
	return t.labelAssociations
}

func (t *stateTask) resourceType() influxdb.ResourceType {
	return influxdb.TasksResourceType
}

func (t *stateTask) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           t.ID(),
		name:         t.parserTask.Name(),
		metaName:     t.parserTask.MetaName(),
		resourceType: t.resourceType(),
		stateStatus:  t.stateStatus,
	}
}

func (t *stateTask) summarize() SummaryTask {
	sum := t.parserTask.summarize()
	sum.ID = SafeID(t.id)
	sum.LabelAssociations = stateToSummaryLabels(t.labelAssociations)
	return sum
}

type stateTelegraf struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

	parserTelegraf *telegraf
	existing       *influxdb.TelegrafConfig
}

func (t *stateTelegraf) ID() influxdb.ID {
	if !IsNew(t.stateStatus) && t.existing != nil {
		return t.existing.ID
	}
	return t.id
}

func (t *stateTelegraf) diffTelegraf() DiffTelegraf {
	return DiffTelegraf{
		DiffIdentifier: DiffIdentifier{
			Kind:        KindTelegraf,
			ID:          SafeID(t.ID()),
			StateStatus: t.stateStatus,
			MetaName:    t.parserTelegraf.MetaName(),
		},
		New: t.parserTelegraf.config,
		Old: t.existing,
	}
}

func (t *stateTelegraf) labels() []*stateLabel {
	return t.labelAssociations
}

func (t *stateTelegraf) resourceType() influxdb.ResourceType {
	return influxdb.TelegrafsResourceType
}

func (t *stateTelegraf) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           t.ID(),
		name:         t.parserTelegraf.Name(),
		metaName:     t.parserTelegraf.MetaName(),
		resourceType: t.resourceType(),
		stateStatus:  t.stateStatus,
	}
}

func (t *stateTelegraf) summarize() SummaryTelegraf {
	sum := t.parserTelegraf.summarize()
	sum.TelegrafConfig.ID = t.id
	sum.TelegrafConfig.OrgID = t.orgID
	sum.LabelAssociations = stateToSummaryLabels(t.labelAssociations)
	return sum
}

type stateVariable struct {
	id, orgID         influxdb.ID
	stateStatus       StateStatus
	labelAssociations []*stateLabel

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
			Kind:        KindVariable,
			ID:          SafeID(v.ID()),
			StateStatus: v.stateStatus,
			MetaName:    v.parserVar.MetaName(),
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

func (v *stateVariable) labels() []*stateLabel {
	return v.labelAssociations
}

func (v *stateVariable) resourceType() influxdb.ResourceType {
	return KindVariable.ResourceType()
}

func (v *stateVariable) shouldApply() bool {
	return IsRemoval(v.stateStatus) ||
		v.existing == nil ||
		v.existing.Description != v.parserVar.Description ||
		!reflect.DeepEqual(v.existing.Selected, v.parserVar.Selected()) ||
		v.existing.Arguments == nil ||
		!reflect.DeepEqual(v.existing.Arguments, v.parserVar.influxVarArgs())
}

func (v *stateVariable) stateIdentity() stateIdentity {
	return stateIdentity{
		id:           v.ID(),
		name:         v.parserVar.Name(),
		metaName:     v.parserVar.MetaName(),
		resourceType: v.resourceType(),
		stateStatus:  v.stateStatus,
	}
}

func (v *stateVariable) summarize() SummaryVariable {
	sum := v.parserVar.summarize()
	sum.ID = SafeID(v.ID())
	sum.OrgID = SafeID(v.orgID)
	sum.LabelAssociations = stateToSummaryLabels(v.labelAssociations)
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

type resourceActions struct {
	skipKinds     map[Kind]bool
	skipResources map[ActionSkipResource]bool
}

func (r resourceActions) skipResource(k Kind, metaName string) bool {
	key := ActionSkipResource{
		Kind:     k,
		MetaName: metaName,
	}
	return r.skipResources[key] || r.skipKinds[k]
}

func isOwner(templateStackID, stackOwnerID influxdb.ID) bool {
	return stackOwnerID == 0 || templateStackID == stackOwnerID
}
