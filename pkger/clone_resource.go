package pkger

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/v2"
	ierrors "github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkger/internal/wordplay"
	"github.com/influxdata/influxdb/v2/snowflake"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
)

var idGenerator = snowflake.NewDefaultIDGenerator()

// NameGenerator generates a random name. Includes an optional fuzz option to
// further randomize the name.
type NameGenerator func() string

// ResourceToClone is a resource that will be cloned.
type ResourceToClone struct {
	Kind Kind        `json:"kind"`
	ID   platform.ID `json:"id,omitempty"`
	Name string      `json:"name"`
	// note(jsteenb2): For time being we'll allow this internally, but not externally. A lot of
	// issues to account for when exposing this to the outside world. Not something I'm keen
	// to accommodate at this time.
	MetaName string `json:"-"`
}

// OK validates a resource clone is viable.
func (r ResourceToClone) OK() error {
	if err := r.Kind.OK(); err != nil {
		return err
	}
	if r.ID == platform.ID(0) && len(r.Name) == 0 {
		return errors.New("must provide an ID or name")
	}
	return nil
}

var kindPriorities = map[Kind]int{
	KindLabel:                         1,
	KindBucket:                        2,
	KindCheck:                         3,
	KindCheckDeadman:                  4,
	KindCheckThreshold:                5,
	KindNotificationEndpoint:          6,
	KindNotificationEndpointHTTP:      7,
	KindNotificationEndpointPagerDuty: 8,
	KindNotificationEndpointSlack:     9,
	KindNotificationRule:              10,
	KindTask:                          11,
	KindVariable:                      12,
	KindDashboard:                     13,
	KindTelegraf:                      14,
}

type exportKey struct {
	orgID platform.ID
	id    platform.ID
	name  string
	kind  Kind
}

func newExportKey(orgID, id platform.ID, k Kind, name string) exportKey {
	return exportKey{
		orgID: orgID,
		id:    id,
		name:  name,
		kind:  k,
	}
}

type resourceExporter struct {
	nameGen NameGenerator

	bucketSVC   influxdb.BucketService
	checkSVC    influxdb.CheckService
	dashSVC     influxdb.DashboardService
	labelSVC    influxdb.LabelService
	endpointSVC influxdb.NotificationEndpointService
	ruleSVC     influxdb.NotificationRuleStore
	taskSVC     taskmodel.TaskService
	teleSVC     influxdb.TelegrafConfigStore
	varSVC      influxdb.VariableService

	mObjects        map[exportKey]Object
	mPkgNames       map[string]bool
	mStackResources map[exportKey]StackResource
}

func newResourceExporter(svc *Service) *resourceExporter {
	return &resourceExporter{
		nameGen:         wordplay.GetRandomName,
		bucketSVC:       svc.bucketSVC,
		checkSVC:        svc.checkSVC,
		dashSVC:         svc.dashSVC,
		labelSVC:        svc.labelSVC,
		endpointSVC:     svc.endpointSVC,
		ruleSVC:         svc.ruleSVC,
		taskSVC:         svc.taskSVC,
		teleSVC:         svc.teleSVC,
		varSVC:          svc.varSVC,
		mObjects:        make(map[exportKey]Object),
		mPkgNames:       make(map[string]bool),
		mStackResources: make(map[exportKey]StackResource),
	}
}

func (ex *resourceExporter) Export(ctx context.Context, resourcesToClone []ResourceToClone, labelNames ...string) error {
	mLabelIDsToMetaName := make(map[platform.ID]string)
	for _, r := range resourcesToClone {
		if !r.Kind.is(KindLabel) || r.MetaName == "" {
			continue
		}
		mLabelIDsToMetaName[r.ID] = r.MetaName
	}

	cloneAssFn, err := ex.resourceCloneAssociationsGen(ctx, mLabelIDsToMetaName, labelNames...)
	if err != nil {
		return err
	}

	resourcesToClone = uniqResourcesToClone(resourcesToClone)
	// sorting this in priority order guarantees that the dependencies/associations
	// for a resource are handled prior to the resource being processed.
	// 	i.e. if a bucket depends on a label, then labels need to be run first
	//		to guarantee they are available before a bucket is exported.
	sort.Slice(resourcesToClone, func(i, j int) bool {
		iName, jName := resourcesToClone[i].Name, resourcesToClone[j].Name
		iKind, jKind := resourcesToClone[i].Kind, resourcesToClone[j].Kind

		if iKind.is(jKind) {
			return iName < jName
		}
		return kindPriorities[iKind] < kindPriorities[jKind]
	})

	for _, r := range resourcesToClone {
		err := ex.resourceCloneToKind(ctx, r, cloneAssFn)
		if err != nil {
			return internalErr(fmt.Errorf("failed to clone resource: resource_id=%s resource_kind=%s err=%q", r.ID, r.Kind, err))
		}
	}

	return nil
}

func (ex *resourceExporter) Objects() []Object {
	objects := make([]Object, 0, len(ex.mObjects))
	for _, obj := range ex.mObjects {
		objects = append(objects, obj)
	}

	return sortObjects(objects)
}

func (ex *resourceExporter) StackResources() []StackResource {
	resources := make([]StackResource, 0, len(ex.mStackResources))
	for _, res := range ex.mStackResources {
		resources = append(resources, res)
	}
	return resources
}

// we only need an id when we have resources that are not unique by name via the
// metastore. resoureces that are unique by name will be provided a default stamp
// making looksup unique since each resource will be unique by name.
const uniqByNameResID = platform.ID(0)

type cloneAssociationsFn func(context.Context, ResourceToClone) (associations []ObjectAssociation, skipResource bool, err error)

func (ex *resourceExporter) resourceCloneToKind(ctx context.Context, r ResourceToClone, cFn cloneAssociationsFn) (e error) {
	defer func() {
		if e != nil {
			e = ierrors.Wrap(e, "cloning resource")
		}
	}()

	ass, skipResource, err := cFn(ctx, r)
	if err != nil {
		return err
	}
	if skipResource {
		return nil
	}

	mapResource := func(orgID, uniqResID platform.ID, k Kind, object Object) {
		// overwrite the default metadata.name field with export generated one here
		metaName := r.MetaName
		if r.MetaName == "" {
			metaName = ex.uniqName()
		}

		stackResource := StackResource{
			APIVersion: APIVersion,
			ID:         r.ID,
			MetaName:   metaName,
			Kind:       r.Kind,
		}
		for _, a := range ass {
			stackResource.Associations = append(stackResource.Associations, StackResourceAssociation(a))
		}

		object.SetMetadataName(metaName)
		object.AddAssociations(ass...)
		key := newExportKey(orgID, uniqResID, k, object.Spec.stringShort(fieldName))
		ex.mObjects[key] = object
		ex.mStackResources[key] = stackResource
	}

	switch {
	case r.Kind.is(KindBucket):
		filter := influxdb.BucketFilter{}
		if r.ID != platform.ID(0) {
			filter.ID = &r.ID
		}
		if len(r.Name) > 0 {
			filter.Name = &r.Name
		}

		bkts, n, err := ex.bucketSVC.FindBuckets(ctx, filter)
		if err != nil {
			return err
		}
		if n < 1 {
			return errors.New("no buckets found")
		}

		for _, bkt := range bkts {
			mapResource(bkt.OrgID, bkt.ID, KindBucket, BucketToObject(r.Name, *bkt))
		}
	case r.Kind.is(KindCheck), r.Kind.is(KindCheckDeadman), r.Kind.is(KindCheckThreshold):
		filter := influxdb.CheckFilter{}
		if r.ID != platform.ID(0) {
			filter.ID = &r.ID
		}
		if len(r.Name) > 0 {
			filter.Name = &r.Name
		}
		chs, n, err := ex.checkSVC.FindChecks(ctx, filter)
		if err != nil {
			return err
		}
		if n < 1 {
			return errors.New("no checks found")
		}

		for _, ch := range chs {
			mapResource(ch.GetOrgID(), ch.GetID(), KindCheck, CheckToObject(r.Name, ch))
		}
	case r.Kind.is(KindDashboard):
		var (
			hasID  bool
			filter = influxdb.DashboardFilter{}
		)
		if r.ID != platform.ID(0) {
			hasID = true
			filter.IDs = []*platform.ID{&r.ID}
		}

		dashes, _, err := ex.dashSVC.FindDashboards(ctx, filter, influxdb.DefaultDashboardFindOptions)
		if err != nil {
			return err
		}

		var mapped bool
		for _, dash := range dashes {
			if (!hasID && len(r.Name) > 0 && dash.Name != r.Name) || (hasID && dash.ID != r.ID) {
				continue
			}

			for _, cell := range dash.Cells {
				v, err := ex.dashSVC.GetDashboardCellView(ctx, dash.ID, cell.ID)
				if err != nil {
					continue
				}
				cell.View = v
			}

			mapResource(dash.OrganizationID, dash.ID, KindDashboard, DashboardToObject(r.Name, *dash))
			mapped = true
		}

		if !mapped {
			return errors.New("no dashboards found")
		}
	case r.Kind.is(KindLabel):
		switch {
		case r.ID != platform.ID(0):
			l, err := ex.labelSVC.FindLabelByID(ctx, r.ID)
			if err != nil {
				return err
			}

			mapResource(l.OrgID, uniqByNameResID, KindLabel, LabelToObject(r.Name, *l))
		case len(r.Name) > 0:
			labels, err := ex.labelSVC.FindLabels(ctx, influxdb.LabelFilter{Name: r.Name})
			if err != nil {
				return err
			}

			for _, l := range labels {
				mapResource(l.OrgID, uniqByNameResID, KindLabel, LabelToObject(r.Name, *l))
			}
		}
	case r.Kind.is(KindNotificationEndpoint),
		r.Kind.is(KindNotificationEndpointHTTP),
		r.Kind.is(KindNotificationEndpointPagerDuty),
		r.Kind.is(KindNotificationEndpointSlack):
		var endpoints []influxdb.NotificationEndpoint

		switch {
		case r.ID != platform.ID(0):
			notifEndpoint, err := ex.endpointSVC.FindNotificationEndpointByID(ctx, r.ID)
			if err != nil {
				return err
			}
			endpoints = append(endpoints, notifEndpoint)
		case len(r.Name) != 0:
			allEndpoints, _, err := ex.endpointSVC.FindNotificationEndpoints(ctx, influxdb.NotificationEndpointFilter{})
			if err != nil {
				return err
			}

			for _, notifEndpoint := range allEndpoints {
				if notifEndpoint.GetName() != r.Name || notifEndpoint == nil {
					continue
				}
				endpoints = append(endpoints, notifEndpoint)
			}
		}

		if len(endpoints) == 0 {
			return errors.New("no notification endpoints found")
		}

		for _, e := range endpoints {
			mapResource(e.GetOrgID(), uniqByNameResID, KindNotificationEndpoint, NotificationEndpointToObject(r.Name, e))
		}
	case r.Kind.is(KindNotificationRule):
		var rules []influxdb.NotificationRule

		switch {
		case r.ID != platform.ID(0):
			r, err := ex.ruleSVC.FindNotificationRuleByID(ctx, r.ID)
			if err != nil {
				return err
			}
			rules = append(rules, r)
		case len(r.Name) != 0:
			allRules, _, err := ex.ruleSVC.FindNotificationRules(ctx, influxdb.NotificationRuleFilter{})
			if err != nil {
				return err
			}

			for _, rule := range allRules {
				if rule.GetName() != r.Name {
					continue
				}
				rules = append(rules, rule)
			}
		}

		if len(rules) == 0 {
			return errors.New("no notification rules found")
		}

		for _, rule := range rules {
			ruleEndpoint, err := ex.endpointSVC.FindNotificationEndpointByID(ctx, rule.GetEndpointID())
			if err != nil {
				return err
			}

			endpointKey := newExportKey(ruleEndpoint.GetOrgID(), uniqByNameResID, KindNotificationEndpoint, ruleEndpoint.GetName())
			object, ok := ex.mObjects[endpointKey]
			if !ok {
				mapResource(ruleEndpoint.GetOrgID(), uniqByNameResID, KindNotificationEndpoint, NotificationEndpointToObject("", ruleEndpoint))
				object = ex.mObjects[endpointKey]
			}
			endpointObjectName := object.Name()

			mapResource(rule.GetOrgID(), rule.GetID(), KindNotificationRule, NotificationRuleToObject(r.Name, endpointObjectName, rule))
		}
	case r.Kind.is(KindTask):
		switch {
		case r.ID != platform.ID(0):
			t, err := ex.taskSVC.FindTaskByID(ctx, r.ID)
			if err != nil {
				return err
			}
			mapResource(t.OrganizationID, t.ID, KindTask, TaskToObject(r.Name, *t))
		case len(r.Name) > 0:
			tasks, n, err := ex.taskSVC.FindTasks(ctx, taskmodel.TaskFilter{Name: &r.Name})
			if err != nil {
				return err
			}
			if n < 1 {
				return errors.New("no tasks found")
			}

			for _, t := range tasks {
				mapResource(t.OrganizationID, t.ID, KindTask, TaskToObject(r.Name, *t))
			}
		}
	case r.Kind.is(KindTelegraf):
		switch {
		case r.ID != platform.ID(0):
			t, err := ex.teleSVC.FindTelegrafConfigByID(ctx, r.ID)
			if err != nil {
				return err
			}
			mapResource(t.OrgID, t.ID, KindTelegraf, TelegrafToObject(r.Name, *t))
		case len(r.Name) > 0:
			telegrafs, _, err := ex.teleSVC.FindTelegrafConfigs(ctx, influxdb.TelegrafConfigFilter{})
			if err != nil {
				return err
			}

			var mapped bool
			for _, t := range telegrafs {
				if t.Name != r.Name {
					continue
				}

				mapResource(t.OrgID, t.ID, KindTelegraf, TelegrafToObject(r.Name, *t))
				mapped = true
			}
			if !mapped {
				return errors.New("no telegraf configs found")
			}

		}
	case r.Kind.is(KindVariable):
		switch {
		case r.ID != platform.ID(0):
			v, err := ex.varSVC.FindVariableByID(ctx, r.ID)
			if err != nil {
				return err
			}
			mapResource(v.OrganizationID, uniqByNameResID, KindVariable, VariableToObject(r.Name, *v))
		case len(r.Name) > 0:
			variables, err := ex.varSVC.FindVariables(ctx, influxdb.VariableFilter{})
			if err != nil {
				return err
			}

			var mapped bool
			for _, v := range variables {
				if v.Name != r.Name {
					continue
				}

				mapResource(v.OrganizationID, uniqByNameResID, KindVariable, VariableToObject(r.Name, *v))
				mapped = true
			}
			if !mapped {
				return errors.New("no variables found")
			}
		}
	default:
		return errors.New("unsupported kind provided: " + string(r.Kind))
	}

	return nil
}

func (ex *resourceExporter) resourceCloneAssociationsGen(ctx context.Context, labelIDsToMetaName map[platform.ID]string, labelNames ...string) (cloneAssociationsFn, error) {
	mLabelNames := make(map[string]bool)
	for _, labelName := range labelNames {
		mLabelNames[labelName] = true
	}

	mLabelIDs, err := getLabelIDMap(ctx, ex.labelSVC, labelNames)
	if err != nil {
		return nil, err
	}

	cloneFn := func(ctx context.Context, r ResourceToClone) ([]ObjectAssociation, bool, error) {
		if r.Kind.is(KindUnknown) {
			return nil, true, nil
		}
		if r.Kind.is(KindLabel) {
			// check here verifies the label maps to an id of a valid label name
			shouldSkip := len(mLabelIDs) > 0 && !mLabelIDs[r.ID]
			return nil, shouldSkip, nil
		}

		if len(r.Name) > 0 && r.ID == platform.ID(0) {
			return nil, false, nil
		}

		labels, err := ex.labelSVC.FindResourceLabels(ctx, influxdb.LabelMappingFilter{
			ResourceID:   r.ID,
			ResourceType: r.Kind.ResourceType(),
		})
		if err != nil {
			return nil, false, ierrors.Wrap(err, "finding resource labels")
		}

		if len(mLabelNames) > 0 {
			shouldSkip := true
			for _, l := range labels {
				if _, ok := mLabelNames[l.Name]; ok {
					shouldSkip = false
					break
				}
			}
			if shouldSkip {
				return nil, true, nil
			}
		}

		var associations []ObjectAssociation
		for _, l := range labels {
			if len(mLabelNames) > 0 {
				if _, ok := mLabelNames[l.Name]; !ok {
					continue
				}
			}

			labelObject := LabelToObject("", *l)
			metaName := labelIDsToMetaName[l.ID]
			if metaName == "" {
				metaName = ex.uniqName()
			}
			labelObject.Metadata[fieldName] = metaName

			k := newExportKey(l.OrgID, uniqByNameResID, KindLabel, l.Name)
			existing, ok := ex.mObjects[k]
			if ok {
				associations = append(associations, ObjectAssociation{
					Kind:     KindLabel,
					MetaName: existing.Name(),
				})
				continue
			}
			associations = append(associations, ObjectAssociation{
				Kind:     KindLabel,
				MetaName: labelObject.Name(),
			})
			ex.mObjects[k] = labelObject
		}
		sort.Slice(associations, func(i, j int) bool {
			return associations[i].MetaName < associations[j].MetaName
		})
		return associations, false, nil
	}

	return cloneFn, nil
}

func (ex *resourceExporter) uniqName() string {
	return uniqMetaName(ex.nameGen, idGenerator, ex.mPkgNames)
}

func uniqMetaName(nameGen NameGenerator, idGen platform.IDGenerator, existingNames map[string]bool) string {
	uuid := strings.ToLower(idGen.ID().String())
	name := uuid
	for i := 1; i < 250; i++ {
		name = fmt.Sprintf("%s-%s", nameGen(), uuid[10:])
		if !existingNames[name] {
			break
		}
	}
	return name
}

func uniqResourcesToClone(resources []ResourceToClone) []ResourceToClone {
	type key struct {
		kind Kind
		id   platform.ID
	}
	m := make(map[key]ResourceToClone)

	for i := range resources {
		r := resources[i]
		rKey := key{kind: r.Kind, id: r.ID}

		kr, ok := m[rKey]
		switch {
		case ok && kr.Name == r.Name && kr.MetaName == r.MetaName:
		case ok && kr.MetaName != "" && r.MetaName == "":
		case ok && kr.MetaName == "" && kr.Name != "" && r.Name == "":
		default:
			m[rKey] = r
		}
	}

	out := make([]ResourceToClone, 0, len(resources))
	for _, r := range m {
		out = append(out, r)
	}
	return out
}

// BucketToObject converts a influxdb.Bucket into an Object.
func BucketToObject(name string, bkt influxdb.Bucket) Object {
	if name == "" {
		name = bkt.Name
	}

	o := newObject(KindBucket, name)
	assignNonZeroStrings(o.Spec, map[string]string{fieldDescription: bkt.Description})
	if bkt.RetentionPeriod != 0 {
		o.Spec[fieldBucketRetentionRules] = retentionRules{newRetentionRule(bkt.RetentionPeriod)}
	}
	return o
}

func CheckToObject(name string, ch influxdb.Check) Object {
	if name == "" {
		name = ch.GetName()
	}
	o := newObject(KindCheck, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldDescription: ch.GetDescription(),
		fieldStatus:      taskmodel.TaskStatusActive,
	})

	assignBase := func(base icheck.Base) {
		o.Spec[fieldQuery] = strings.TrimSpace(base.Query.Text)
		o.Spec[fieldCheckStatusMessageTemplate] = base.StatusMessageTemplate
		assignNonZeroFluxDurs(o.Spec, map[string]*notification.Duration{
			fieldEvery:  base.Every,
			fieldOffset: base.Offset,
		})

		var tags []Resource
		for _, t := range base.Tags {
			if t.Valid() != nil {
				continue
			}
			tags = append(tags, Resource{
				fieldKey:   t.Key,
				fieldValue: t.Value,
			})
		}
		if len(tags) > 0 {
			o.Spec[fieldCheckTags] = tags
		}
	}

	switch cT := ch.(type) {
	case *icheck.Deadman:
		o.Kind = KindCheckDeadman
		assignBase(cT.Base)
		assignNonZeroFluxDurs(o.Spec, map[string]*notification.Duration{
			fieldCheckTimeSince: cT.TimeSince,
			fieldCheckStaleTime: cT.StaleTime,
		})
		o.Spec[fieldLevel] = cT.Level.String()
		assignNonZeroBools(o.Spec, map[string]bool{fieldCheckReportZero: cT.ReportZero})
	case *icheck.Threshold:
		o.Kind = KindCheckThreshold
		assignBase(cT.Base)
		var thresholds []Resource
		for _, th := range cT.Thresholds {
			thresholds = append(thresholds, convertThreshold(th))
		}
		o.Spec[fieldCheckThresholds] = thresholds
	}
	return o
}

func convertThreshold(th icheck.ThresholdConfig) Resource {
	r := Resource{fieldLevel: th.GetLevel().String()}

	assignLesser := func(threshType thresholdType, allValues bool, val float64) {
		r[fieldType] = string(threshType)
		assignNonZeroBools(r, map[string]bool{fieldCheckAllValues: allValues})
		r[fieldValue] = val
	}

	switch realType := th.(type) {
	case icheck.Lesser:
		assignLesser(thresholdTypeLesser, realType.AllValues, realType.Value)
	case *icheck.Lesser:
		assignLesser(thresholdTypeLesser, realType.AllValues, realType.Value)
	case icheck.Greater:
		assignLesser(thresholdTypeGreater, realType.AllValues, realType.Value)
	case *icheck.Greater:
		assignLesser(thresholdTypeGreater, realType.AllValues, realType.Value)
	case icheck.Range:
		assignRangeThreshold(r, realType)
	case *icheck.Range:
		assignRangeThreshold(r, *realType)
	}

	return r
}

func assignRangeThreshold(r Resource, rangeThreshold icheck.Range) {
	thType := thresholdTypeOutsideRange
	if rangeThreshold.Within {
		thType = thresholdTypeInsideRange
	}
	r[fieldType] = string(thType)
	assignNonZeroBools(r, map[string]bool{fieldCheckAllValues: rangeThreshold.AllValues})
	r[fieldMax] = rangeThreshold.Max
	r[fieldMin] = rangeThreshold.Min
}

func convertCellView(cell influxdb.Cell) chart {
	var name string
	if cell.View != nil {
		name = cell.View.Name
	}
	ch := chart{
		Name:   name,
		Height: int(cell.H),
		Width:  int(cell.W),
		XPos:   int(cell.X),
		YPos:   int(cell.Y),
	}

	setCommon := func(k chartKind, iColors []influxdb.ViewColor, dec influxdb.DecimalPlaces, iQueries []influxdb.DashboardQuery) {
		ch.Kind = k
		ch.Colors = convertColors(iColors)
		ch.DecimalPlaces = int(dec.Digits)
		ch.EnforceDecimals = dec.IsEnforced
		ch.Queries = convertQueries(iQueries)
	}

	setNoteFixes := func(note string, noteOnEmpty bool, prefix, suffix string) {
		ch.Note = note
		ch.NoteOnEmpty = noteOnEmpty
		ch.Prefix = prefix
		ch.Suffix = suffix
	}

	setStaticLegend := func(sl influxdb.StaticLegend) {
		ch.StaticLegend.ColorizeRows = sl.ColorizeRows
		ch.StaticLegend.HeightRatio = sl.HeightRatio
		ch.StaticLegend.Hide = sl.Hide
		ch.StaticLegend.Opacity = sl.Opacity
		ch.StaticLegend.OrientationThreshold = sl.OrientationThreshold
		ch.StaticLegend.ValueAxis = sl.ValueAxis
		ch.StaticLegend.WidthRatio = sl.WidthRatio
	}

	props := cell.View.Properties
	switch p := props.(type) {
	case influxdb.GaugeViewProperties:
		setCommon(chartKindGauge, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.TickPrefix = p.TickPrefix
		ch.TickSuffix = p.TickSuffix
	case influxdb.GeoViewProperties:
		ch.Kind = chartKindGeo
		ch.Queries = convertQueries(p.Queries)
		ch.Zoom = p.Zoom
		ch.Center = center{Lat: p.Center.Lat, Lon: p.Center.Lon}
		ch.MapStyle = p.MapStyle
		ch.AllowPanAndZoom = p.AllowPanAndZoom
		ch.DetectCoordinateFields = p.DetectCoordinateFields
		ch.Colors = convertColors(p.ViewColor)
		ch.GeoLayers = convertGeoLayers(p.GeoLayers)
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
	case influxdb.HeatmapViewProperties:
		ch.Kind = chartKindHeatMap
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YCol = p.YColumn
		ch.GenerateYAxisTicks = p.GenerateYAxisTicks
		ch.YTotalTicks = p.YTotalTicks
		ch.YTickStart = p.YTickStart
		ch.YTickStep = p.YTickStep
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
		ch.BinSize = int(p.BinSize)
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.HistogramViewProperties:
		ch.Kind = chartKindHistogram
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = convertColors(p.ViewColors)
		ch.FillColumns = p.FillColumns
		ch.XCol = p.XColumn
		ch.Axes = []axis{{Label: p.XAxisLabel, Name: "x", Domain: p.XDomain}}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
		ch.BinCount = p.BinCount
		ch.Position = p.Position
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.MarkdownViewProperties:
		ch.Kind = chartKindMarkdown
		ch.Note = p.Note
	case influxdb.LinePlusSingleStatProperties:
		setCommon(chartKindSingleStatPlusLine, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.StaticLegend = StaticLegend{}
		setStaticLegend(p.StaticLegend)
		ch.Axes = convertAxes(p.Axes)
		ch.Shade = p.ShadeBelow
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YCol = p.YColumn
		ch.GenerateYAxisTicks = p.GenerateYAxisTicks
		ch.YTotalTicks = p.YTotalTicks
		ch.YTickStart = p.YTickStart
		ch.YTickStep = p.YTickStep
		ch.Position = p.Position
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.SingleStatViewProperties:
		setCommon(chartKindSingleStat, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.TickPrefix = p.TickPrefix
		ch.TickSuffix = p.TickSuffix
	case influxdb.MosaicViewProperties:
		ch.Kind = chartKindMosaic
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YLabelColumnSeparator = p.YLabelColumnSeparator
		ch.YLabelColumns = p.YLabelColumns
		ch.YSeriesColumns = p.YSeriesColumns
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.ScatterViewProperties:
		ch.Kind = chartKindScatter
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YCol = p.YColumn
		ch.GenerateYAxisTicks = p.GenerateYAxisTicks
		ch.YTotalTicks = p.YTotalTicks
		ch.YTickStart = p.YTickStart
		ch.YTickStep = p.YTickStep
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.TableViewProperties:
		setCommon(chartKindTable, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		ch.TimeFormat = p.TimeFormat
		ch.TableOptions = tableOptions{
			VerticalTimeAxis: p.TableOptions.VerticalTimeAxis,
			SortByField:      p.TableOptions.SortBy.InternalName,
			Wrapping:         p.TableOptions.Wrapping,
			FixFirstColumn:   p.TableOptions.FixFirstColumn,
		}
		for _, fieldOpt := range p.FieldOptions {
			ch.FieldOptions = append(ch.FieldOptions, fieldOption{
				FieldName:   fieldOpt.InternalName,
				DisplayName: fieldOpt.DisplayName,
				Visible:     fieldOpt.Visible,
			})
		}
	case influxdb.BandViewProperties:
		setCommon(chartKindBand, p.ViewColors, influxdb.DecimalPlaces{}, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		ch.StaticLegend = StaticLegend{}
		setStaticLegend(p.StaticLegend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YCol = p.YColumn
		ch.GenerateYAxisTicks = p.GenerateYAxisTicks
		ch.YTotalTicks = p.YTotalTicks
		ch.YTickStart = p.YTickStart
		ch.YTickStep = p.YTickStep
		ch.UpperColumn = p.UpperColumn
		ch.MainColumn = p.MainColumn
		ch.LowerColumn = p.LowerColumn
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	case influxdb.XYViewProperties:
		setCommon(chartKindXY, p.ViewColors, influxdb.DecimalPlaces{}, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		ch.StaticLegend = StaticLegend{}
		setStaticLegend(p.StaticLegend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.Shade = p.ShadeBelow
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.GenerateXAxisTicks = p.GenerateXAxisTicks
		ch.XTotalTicks = p.XTotalTicks
		ch.XTickStart = p.XTickStart
		ch.XTickStep = p.XTickStep
		ch.YCol = p.YColumn
		ch.GenerateYAxisTicks = p.GenerateYAxisTicks
		ch.YTotalTicks = p.YTotalTicks
		ch.YTickStart = p.YTickStart
		ch.YTickStep = p.YTickStep
		ch.Position = p.Position
		ch.LegendColorizeRows = p.LegendColorizeRows
		ch.LegendHide = p.LegendHide
		ch.LegendOpacity = float64(p.LegendOpacity)
		ch.LegendOrientationThreshold = int(p.LegendOrientationThreshold)
	}

	sort.Slice(ch.Axes, func(i, j int) bool {
		return ch.Axes[i].Name < ch.Axes[j].Name
	})
	return ch
}

func convertChartToResource(ch chart) Resource {
	r := Resource{
		fieldKind:        ch.Kind.title(),
		fieldName:        ch.Name,
		fieldChartHeight: ch.Height,
		fieldChartWidth:  ch.Width,
	}
	var qq []Resource
	for _, q := range ch.Queries {
		qq = append(qq, Resource{
			fieldQuery: q.DashboardQuery(),
		})
	}
	if len(qq) > 0 {
		r[fieldChartQueries] = qq
	}
	if len(ch.Colors) > 0 {
		r[fieldChartColors] = ch.Colors
	}
	if len(ch.Axes) > 0 {
		r[fieldChartAxes] = ch.Axes
	}
	if len(ch.YLabelColumns) > 0 {
		r[fieldChartYLabelColumns] = ch.YLabelColumns
	}
	if len(ch.YSeriesColumns) > 0 {
		r[fieldChartYSeriesColumns] = ch.YSeriesColumns
	}
	if len(ch.UpperColumn) > 0 {
		r[fieldChartUpperColumn] = ch.UpperColumn
	}
	if len(ch.MainColumn) > 0 {
		r[fieldChartMainColumn] = ch.MainColumn
	}
	if len(ch.LowerColumn) > 0 {
		r[fieldChartLowerColumn] = ch.LowerColumn
	}
	if ch.EnforceDecimals {
		r[fieldChartDecimalPlaces] = ch.DecimalPlaces
	}

	if len(ch.FillColumns) > 0 {
		r[fieldChartFillColumns] = ch.FillColumns
	}

	if len(ch.GenerateXAxisTicks) > 0 {
		r[fieldChartGenerateXAxisTicks] = ch.GenerateXAxisTicks
	}

	if len(ch.GenerateYAxisTicks) > 0 {
		r[fieldChartGenerateYAxisTicks] = ch.GenerateYAxisTicks
	}

	if ch.StaticLegend.HeightRatio >= 0 && ch.StaticLegend.WidthRatio >= 0 {
		r[fieldChartStaticLegend] = ch.StaticLegend
	}

	if len(ch.GeoLayers) > 0 {
		geoLayers := make([]Resource, 0, len(ch.GeoLayers))
		for _, l := range ch.GeoLayers {
			lRes := make(Resource)
			geoLayers = append(geoLayers, lRes)
			assignNonZeroStrings(lRes, map[string]string{
				fieldChartGeoLayerType:           l.Type,
				fieldChartGeoLayerRadiusField:    l.RadiusField,
				fieldChartGeoLayerIntensityField: l.IntensityField,
				fieldChartGeoLayerColorField:     l.ColorField,
			})
			assignNonZeroInts(lRes, map[string]int{
				fieldChartGeoLayerRadius:     int(l.Radius),
				fieldChartGeoLayerBlur:       int(l.Blur),
				fieldChartGeoLayerSpeed:      int(l.Speed),
				fieldChartGeoLayerTrackWidth: int(l.TrackWidth),
			})
			assignNonZeroBools(lRes, map[string]bool{
				fieldChartGeoLayerRandomColors:      l.RandomColors,
				fieldChartGeoLayerIsClustered:       l.IsClustered,
				fieldChartGeoLayerInterpolateColors: l.InterpolateColors,
			})
			if len(l.ViewColors) > 0 {
				lRes[fieldChartGeoLayerViewColors] = l.ViewColors
			}
			if l.RadiusDimension != nil {
				lRes[fieldChartGeoLayerRadiusDimension] = l.RadiusDimension
			}
			if l.ColorDimension != nil {
				lRes[fieldChartGeoLayerColorDimension] = l.ColorDimension
			}
			if l.IntensityDimension != nil {
				lRes[fieldChartGeoLayerIntensityDimension] = l.IntensityDimension
			}
		}
		r[fieldChartGeoLayers] = geoLayers
	}

	if zero := new(tableOptions); ch.TableOptions != *zero {
		tRes := make(Resource)
		assignNonZeroBools(tRes, map[string]bool{
			fieldChartTableOptionVerticalTimeAxis: ch.TableOptions.VerticalTimeAxis,
			fieldChartTableOptionFixFirstColumn:   ch.TableOptions.FixFirstColumn,
		})
		assignNonZeroStrings(tRes, map[string]string{
			fieldChartTableOptionSortBy:   ch.TableOptions.SortByField,
			fieldChartTableOptionWrapping: ch.TableOptions.Wrapping,
		})
		r[fieldChartTableOptions] = tRes
	}

	if len(ch.FieldOptions) > 0 {
		fieldOpts := make([]Resource, 0, len(ch.FieldOptions))
		for _, fo := range ch.FieldOptions {
			fRes := make(Resource)
			assignNonZeroBools(fRes, map[string]bool{
				fieldChartFieldOptionVisible: fo.Visible,
			})
			assignNonZeroStrings(fRes, map[string]string{
				fieldChartFieldOptionDisplayName: fo.DisplayName,
				fieldChartFieldOptionFieldName:   fo.FieldName,
			})
			fieldOpts = append(fieldOpts, fRes)
		}
		r[fieldChartFieldOptions] = fieldOpts
	}

	assignNonZeroBools(r, map[string]bool{
		fieldChartNoteOnEmpty:               ch.NoteOnEmpty,
		fieldChartShade:                     ch.Shade,
		fieldChartLegendColorizeRows:        ch.LegendColorizeRows,
		fieldChartLegendHide:                ch.LegendHide,
		fieldChartStaticLegendColorizeRows:  ch.StaticLegend.ColorizeRows,
		fieldChartStaticLegendHide:          ch.StaticLegend.Hide,
		fieldChartGeoAllowPanAndZoom:        ch.AllowPanAndZoom,
		fieldChartGeoDetectCoordinateFields: ch.DetectCoordinateFields,
	})

	assignNonZeroStrings(r, map[string]string{
		fieldChartNote:                  ch.Note,
		fieldPrefix:                     ch.Prefix,
		fieldSuffix:                     ch.Suffix,
		fieldChartGeom:                  ch.Geom,
		fieldChartXCol:                  ch.XCol,
		fieldChartYCol:                  ch.YCol,
		fieldChartPosition:              ch.Position,
		fieldChartTickPrefix:            ch.TickPrefix,
		fieldChartTickSuffix:            ch.TickSuffix,
		fieldChartTimeFormat:            ch.TimeFormat,
		fieldChartHoverDimension:        ch.HoverDimension,
		fieldChartYLabelColumnSeparator: ch.YLabelColumnSeparator,
		fieldChartStaticLegendValueAxis: ch.StaticLegend.ValueAxis,
		fieldChartGeoMapStyle:           ch.MapStyle,
	})

	assignNonZeroInts(r, map[string]int{
		fieldChartXPos:                             ch.XPos,
		fieldChartXTotalTicks:                      ch.XTotalTicks,
		fieldChartYPos:                             ch.YPos,
		fieldChartYTotalTicks:                      ch.YTotalTicks,
		fieldChartBinCount:                         ch.BinCount,
		fieldChartBinSize:                          ch.BinSize,
		fieldChartLegendOrientationThreshold:       ch.LegendOrientationThreshold,
		fieldChartStaticLegendOrientationThreshold: ch.StaticLegend.OrientationThreshold,
	})

	assignNonZeroFloats(r, map[string]float64{
		fieldChartLegendOpacity:           ch.LegendOpacity,
		fieldChartStaticLegendOpacity:     ch.StaticLegend.Opacity,
		fieldChartStaticLegendHeightRatio: ch.StaticLegend.HeightRatio,
		fieldChartStaticLegendWidthRatio:  ch.StaticLegend.WidthRatio,
		fieldChartXTickStart:              ch.XTickStart,
		fieldChartXTickStep:               ch.XTickStep,
		fieldChartYTickStart:              ch.YTickStart,
		fieldChartYTickStep:               ch.YTickStep,
		fieldChartGeoCenterLon:            ch.Center.Lon,
		fieldChartGeoCenterLat:            ch.Center.Lat,
		fieldChartGeoZoom:                 ch.Zoom,
	})

	return r
}

func convertAxis(name string, a influxdb.Axis) *axis {
	return &axis{
		Base:   a.Base,
		Label:  a.Label,
		Name:   name,
		Prefix: a.Prefix,
		Scale:  a.Scale,
		Suffix: a.Suffix,
	}
}

func convertAxes(iAxes map[string]influxdb.Axis) axes {
	out := make(axes, 0, len(iAxes))
	for name, a := range iAxes {
		out = append(out, *convertAxis(name, a))
	}
	return out
}

func convertColors(iColors []influxdb.ViewColor) colors {
	out := make(colors, 0, len(iColors))
	for _, ic := range iColors {
		out = append(out, &color{
			ID:    ic.ID,
			Name:  ic.Name,
			Type:  ic.Type,
			Hex:   ic.Hex,
			Value: flt64Ptr(ic.Value),
		})
	}
	return out
}

func convertQueries(iQueries []influxdb.DashboardQuery) queries {
	out := make(queries, 0, len(iQueries))
	for _, iq := range iQueries {
		out = append(out, query{Query: strings.TrimSpace(iq.Text)})
	}
	return out
}

func convertGeoLayers(iLayers []influxdb.GeoLayer) geoLayers {
	out := make(geoLayers, 0, len(iLayers))
	for _, ic := range iLayers {
		out = append(out, &geoLayer{
			Type:               ic.Type,
			RadiusField:        ic.RadiusField,
			ColorField:         ic.ColorField,
			IntensityField:     ic.IntensityField,
			ViewColors:         convertColors(ic.ViewColors),
			Radius:             ic.Radius,
			Blur:               ic.Blur,
			RadiusDimension:    convertAxis("radius", ic.RadiusDimension),
			ColorDimension:     convertAxis("color", ic.ColorDimension),
			IntensityDimension: convertAxis("intensity", ic.IntensityDimension),
			InterpolateColors:  ic.InterpolateColors,
			TrackWidth:         ic.TrackWidth,
			Speed:              ic.Speed,
			RandomColors:       ic.RandomColors,
			IsClustered:        ic.IsClustered,
		})
	}
	return out
}

// DashboardToObject converts an influxdb.Dashboard to an Object.
func DashboardToObject(name string, dash influxdb.Dashboard) Object {
	if name == "" {
		name = dash.Name
	}

	sort.Slice(dash.Cells, func(i, j int) bool {
		ic, jc := dash.Cells[i], dash.Cells[j]
		if ic.X == jc.X {
			return ic.Y < jc.Y
		}
		return ic.X < jc.X
	})

	charts := make([]Resource, 0, len(dash.Cells))
	for _, cell := range dash.Cells {
		if cell.View == nil {
			continue
		}
		ch := convertCellView(*cell)
		if !ch.Kind.ok() {
			continue
		}
		charts = append(charts, convertChartToResource(ch))
	}

	o := newObject(KindDashboard, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldDescription: dash.Description,
	})
	o.Spec[fieldDashCharts] = charts
	return o
}

// LabelToObject converts an influxdb.Label to an Object.
func LabelToObject(name string, l influxdb.Label) Object {
	if name == "" {
		name = l.Name
	}

	o := newObject(KindLabel, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldDescription: l.Properties["description"],
		fieldLabelColor:  l.Properties["color"],
	})
	return o
}

// NotificationEndpointToObject converts an notification endpoint into a pkger Object.
func NotificationEndpointToObject(name string, e influxdb.NotificationEndpoint) Object {
	if name == "" {
		name = e.GetName()
	}

	o := newObject(KindNotificationEndpoint, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldDescription: e.GetDescription(),
		fieldStatus:      string(e.GetStatus()),
	})

	switch actual := e.(type) {
	case *endpoint.HTTP:
		o.Kind = KindNotificationEndpointHTTP
		o.Spec[fieldNotificationEndpointHTTPMethod] = actual.Method
		o.Spec[fieldNotificationEndpointURL] = actual.URL
		o.Spec[fieldType] = actual.AuthMethod
		assignNonZeroSecrets(o.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointPassword: actual.Password,
			fieldNotificationEndpointToken:    actual.Token,
			fieldNotificationEndpointUsername: actual.Username,
		})
	case *endpoint.PagerDuty:
		o.Kind = KindNotificationEndpointPagerDuty
		o.Spec[fieldNotificationEndpointURL] = actual.ClientURL
		assignNonZeroSecrets(o.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointRoutingKey: actual.RoutingKey,
		})
	case *endpoint.Slack:
		o.Kind = KindNotificationEndpointSlack
		o.Spec[fieldNotificationEndpointURL] = actual.URL
		assignNonZeroSecrets(o.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointToken: actual.Token,
		})
	}

	return o
}

// NotificationRuleToObject converts an notification rule into a pkger Object.
func NotificationRuleToObject(name, endpointPkgName string, iRule influxdb.NotificationRule) Object {
	if name == "" {
		name = iRule.GetName()
	}

	o := newObject(KindNotificationRule, name)
	o.Spec[fieldNotificationRuleEndpointName] = endpointPkgName
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldDescription: iRule.GetDescription(),
	})

	assignBase := func(base rule.Base) {
		assignNonZeroFluxDurs(o.Spec, map[string]*notification.Duration{
			fieldEvery:  base.Every,
			fieldOffset: base.Offset,
		})

		var tagRes []Resource
		for _, tRule := range base.TagRules {
			tagRes = append(tagRes, Resource{
				fieldKey:      tRule.Key,
				fieldValue:    tRule.Value,
				fieldOperator: tRule.Operator.String(),
			})
		}
		if len(tagRes) > 0 {
			o.Spec[fieldNotificationRuleTagRules] = tagRes
		}

		var statusRuleRes []Resource
		for _, sRule := range base.StatusRules {
			sRes := Resource{
				fieldNotificationRuleCurrentLevel: sRule.CurrentLevel.String(),
			}
			if sRule.PreviousLevel != nil {
				sRes[fieldNotificationRulePreviousLevel] = sRule.PreviousLevel.String()
			}
			statusRuleRes = append(statusRuleRes, sRes)
		}
		if len(statusRuleRes) > 0 {
			o.Spec[fieldNotificationRuleStatusRules] = statusRuleRes
		}
	}

	switch t := iRule.(type) {
	case *rule.HTTP:
		assignBase(t.Base)
	case *rule.PagerDuty:
		assignBase(t.Base)
		o.Spec[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
	case *rule.Slack:
		assignBase(t.Base)
		o.Spec[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
		assignNonZeroStrings(o.Spec, map[string]string{fieldNotificationRuleChannel: t.Channel})
	}

	return o
}

// regex used to rip out the hard coded task option stuffs
var taskFluxRegex = regexp.MustCompile(`option task = {(.|\n)*?}`)

// TaskToObject converts an influxdb.Task into a pkger.Object.
func TaskToObject(name string, t taskmodel.Task) Object {
	if name == "" {
		name = t.Name
	}

	query := strings.TrimSpace(taskFluxRegex.ReplaceAllString(t.Flux, ""))

	o := newObject(KindTask, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldTaskCron:    t.Cron,
		fieldDescription: t.Description,
		fieldEvery:       t.Every,
		fieldOffset:      durToStr(t.Offset),
		fieldQuery:       strings.TrimSpace(query),
	})
	return o
}

// TelegrafToObject converts an influxdb.TelegrafConfig into a pkger.Object.
func TelegrafToObject(name string, t influxdb.TelegrafConfig) Object {
	if name == "" {
		name = t.Name
	}

	o := newObject(KindTelegraf, name)
	assignNonZeroStrings(o.Spec, map[string]string{
		fieldTelegrafConfig: t.Config,
		fieldDescription:    t.Description,
	})
	return o
}

// VariableToObject converts an influxdb.Variable to a pkger.Object.
func VariableToObject(name string, v influxdb.Variable) Object {
	if name == "" {
		name = v.Name
	}

	o := newObject(KindVariable, name)

	assignNonZeroStrings(o.Spec, map[string]string{fieldDescription: v.Description})

	if len(v.Selected) > 0 {
		o.Spec[fieldVariableSelected] = v.Selected
	}

	args := v.Arguments
	if args == nil {
		return o
	}
	o.Spec[fieldType] = args.Type

	switch args.Type {
	case fieldArgTypeConstant:
		vals, ok := args.Values.(influxdb.VariableConstantValues)
		if ok {
			o.Spec[fieldValues] = []string(vals)
		}
	case fieldArgTypeMap:
		vals, ok := args.Values.(influxdb.VariableMapValues)
		if ok {
			o.Spec[fieldValues] = map[string]string(vals)
		}
	case fieldArgTypeQuery:
		vals, ok := args.Values.(influxdb.VariableQueryValues)
		if ok {
			o.Spec[fieldLanguage] = vals.Language
			o.Spec[fieldQuery] = strings.TrimSpace(vals.Query)
		}
	}

	return o
}

func newObject(kind Kind, name string) Object {
	return Object{
		APIVersion: APIVersion,
		Kind:       kind,
		Metadata: Resource{
			// this timestamp is added to make the resource unique. Should also indicate
			// to the end user that this is machine readable and the spec.name field is
			// the one they want to edit when a name change is desired.
			fieldName: strings.ToLower(idGenerator.ID().String()),
		},
		Spec: Resource{
			fieldName: name,
		},
	}
}

func assignNonZeroFluxDurs(r Resource, m map[string]*notification.Duration) {
	for field, dur := range m {
		if dur == nil {
			continue
		}
		if dur.TimeDuration() == 0 {
			continue
		}
		r[field] = dur.TimeDuration().String()
	}
}

func assignNonZeroBools(r Resource, m map[string]bool) {
	for k, v := range m {
		if v {
			r[k] = v
		}
	}
}

func assignNonZeroInts(r Resource, m map[string]int) {
	for k, v := range m {
		if v != 0 {
			r[k] = v
		}
	}
}

func assignNonZeroFloats(r Resource, m map[string]float64) {
	for k, v := range m {
		if v != 0 {
			r[k] = v
		}
	}
}

func assignNonZeroStrings(r Resource, m map[string]string) {
	for k, v := range m {
		if v != "" {
			r[k] = v
		}
	}
}

func assignNonZeroSecrets(r Resource, m map[string]influxdb.SecretField) {
	for field, secret := range m {
		if secret.Key == "" {
			continue
		}
		r[field] = Resource{
			fieldReferencesSecret: Resource{
				fieldKey: secret.Key,
			},
		}
	}
}

func stringsToColors(clrs []string) colors {
	newColors := make(colors, 0)
	for _, x := range clrs {
		newColors = append(newColors, &color{Hex: x})
	}
	return newColors
}
