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
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkger/internal/wordplay"
	"github.com/influxdata/influxdb/v2/snowflake"
)

var idGenerator = snowflake.NewDefaultIDGenerator()

// NameGenerator generates a random name. Includes an optional fuzz option to
// further randomize the name.
type NameGenerator func() string

// ResourceToClone is a resource that will be cloned.
type ResourceToClone struct {
	Kind Kind        `json:"kind"`
	ID   influxdb.ID `json:"id"`
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
	if r.ID == influxdb.ID(0) {
		return errors.New("must provide an ID")
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
	orgID influxdb.ID
	id    influxdb.ID
	name  string
	kind  Kind
}

func newExportKey(orgID, id influxdb.ID, k Kind, name string) exportKey {
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
	taskSVC     influxdb.TaskService
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
	mLabelIDsToMetaName := make(map[influxdb.ID]string)
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

func (ex *resourceExporter) uniqByNameResID() influxdb.ID {
	// we only need an id when we have resources that are not unique by name via the
	// metastore. resoureces that are unique by name will be provided a default stamp
	// making looksup unique since each resource will be unique by name.
	const uniqByNameResID = 0
	return uniqByNameResID
}

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

	mapResource := func(orgID, uniqResID influxdb.ID, k Kind, object Object) {
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

	uniqByNameResID := ex.uniqByNameResID()

	switch {
	case r.Kind.is(KindBucket):
		bkt, err := ex.bucketSVC.FindBucketByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(bkt.OrgID, uniqByNameResID, KindBucket, BucketToObject(r.Name, *bkt))
	case r.Kind.is(KindCheck),
		r.Kind.is(KindCheckDeadman),
		r.Kind.is(KindCheckThreshold):
		ch, err := ex.checkSVC.FindCheckByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(ch.GetOrgID(), uniqByNameResID, KindCheck, CheckToObject(r.Name, ch))
	case r.Kind.is(KindDashboard):
		dash, err := findDashboardByIDFull(ctx, ex.dashSVC, r.ID)
		if err != nil {
			return err
		}
		mapResource(dash.OrganizationID, dash.ID, KindDashboard, DashboardToObject(r.Name, *dash))
	case r.Kind.is(KindLabel):
		l, err := ex.labelSVC.FindLabelByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(l.OrgID, uniqByNameResID, KindLabel, LabelToObject(r.Name, *l))
	case r.Kind.is(KindNotificationEndpoint),
		r.Kind.is(KindNotificationEndpointHTTP),
		r.Kind.is(KindNotificationEndpointPagerDuty),
		r.Kind.is(KindNotificationEndpointSlack):
		e, err := ex.endpointSVC.FindNotificationEndpointByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(e.GetOrgID(), uniqByNameResID, KindNotificationEndpoint, NotificationEndpointToObject(r.Name, e))
	case r.Kind.is(KindNotificationRule):
		rule, ruleEndpoint, err := ex.getEndpointRule(ctx, r.ID)
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
	case r.Kind.is(KindTask):
		t, err := ex.taskSVC.FindTaskByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(t.OrganizationID, t.ID, KindTask, TaskToObject(r.Name, *t))
	case r.Kind.is(KindTelegraf):
		t, err := ex.teleSVC.FindTelegrafConfigByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(t.OrgID, t.ID, KindTelegraf, TelegrafToObject(r.Name, *t))
	case r.Kind.is(KindVariable):
		v, err := ex.varSVC.FindVariableByID(ctx, r.ID)
		if err != nil {
			return err
		}
		mapResource(v.OrganizationID, uniqByNameResID, KindVariable, VariableToObject(r.Name, *v))
	default:
		return errors.New("unsupported kind provided: " + string(r.Kind))
	}

	return nil
}

func (ex *resourceExporter) resourceCloneAssociationsGen(ctx context.Context, labelIDsToMetaName map[influxdb.ID]string, labelNames ...string) (cloneAssociationsFn, error) {
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

			k := newExportKey(l.OrgID, ex.uniqByNameResID(), KindLabel, l.Name)
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

func (ex *resourceExporter) getEndpointRule(ctx context.Context, id influxdb.ID) (influxdb.NotificationRule, influxdb.NotificationEndpoint, error) {
	rule, err := ex.ruleSVC.FindNotificationRuleByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	ruleEndpoint, err := ex.endpointSVC.FindNotificationEndpointByID(ctx, rule.GetEndpointID())
	if err != nil {
		return nil, nil, err
	}

	return rule, ruleEndpoint, nil
}

func (ex *resourceExporter) uniqName() string {
	return uniqMetaName(ex.nameGen, idGenerator, ex.mPkgNames)
}

func uniqMetaName(nameGen NameGenerator, idGen influxdb.IDGenerator, existingNames map[string]bool) string {
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

func findDashboardByIDFull(ctx context.Context, dashSVC influxdb.DashboardService, id influxdb.ID) (*influxdb.Dashboard, error) {
	dash, err := dashSVC.FindDashboardByID(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, cell := range dash.Cells {
		v, err := dashSVC.GetDashboardCellView(ctx, id, cell.ID)
		if err != nil {
			return nil, err
		}
		cell.View = v
	}
	return dash, nil
}

func uniqResourcesToClone(resources []ResourceToClone) []ResourceToClone {
	type key struct {
		kind Kind
		id   influxdb.ID
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
		fieldStatus:      influxdb.TaskStatusActive,
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

	setLegend := func(l influxdb.Legend) {
		ch.Legend.Orientation = l.Orientation
		ch.Legend.Type = l.Type
	}

	props := cell.View.Properties
	switch p := props.(type) {
	case influxdb.GaugeViewProperties:
		setCommon(chartKindGauge, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.TickPrefix = p.TickPrefix
		ch.TickSuffix = p.TickSuffix
	case influxdb.HeatmapViewProperties:
		ch.Kind = chartKindHeatMap
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
		ch.BinSize = int(p.BinSize)
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
	case influxdb.MarkdownViewProperties:
		ch.Kind = chartKindMarkdown
		ch.Note = p.Note
	case influxdb.LinePlusSingleStatProperties:
		setCommon(chartKindSingleStatPlusLine, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Shade = p.ShadeBelow
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Position = p.Position
	case influxdb.SingleStatViewProperties:
		setCommon(chartKindSingleStat, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.TickPrefix = p.TickPrefix
		ch.TickSuffix = p.TickSuffix
	case influxdb.MosaicViewProperties:
		ch.Kind = chartKindMosaic
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.XCol = p.XColumn
		ch.YSeriesColumns = p.YSeriesColumns
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
	case influxdb.ScatterViewProperties:
		ch.Kind = chartKindScatter
		ch.Queries = convertQueries(p.Queries)
		ch.Colors = stringsToColors(p.ViewColors)
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Axes = []axis{
			{Label: p.XAxisLabel, Prefix: p.XPrefix, Suffix: p.XSuffix, Name: "x", Domain: p.XDomain},
			{Label: p.YAxisLabel, Prefix: p.YPrefix, Suffix: p.YSuffix, Name: "y", Domain: p.YDomain},
		}
		ch.Note = p.Note
		ch.NoteOnEmpty = p.ShowNoteWhenEmpty
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
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.UpperColumn = p.UpperColumn
		ch.LowerColumn = p.LowerColumn
	case influxdb.XYViewProperties:
		setCommon(chartKindXY, p.ViewColors, influxdb.DecimalPlaces{}, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.Shade = p.ShadeBelow
		ch.HoverDimension = p.HoverDimension
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Position = p.Position
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
	if len(ch.YSeriesColumns) > 0 {
		r[fieldChartYSeriesColumns] = ch.YSeriesColumns
	}
	if len(ch.UpperColumn) > 0 {
		r[fieldChartUpperColumn] = ch.UpperColumn
	}
	if len(ch.LowerColumn) > 0 {
		r[fieldChartLowerColumn] = ch.LowerColumn
	}
	if ch.EnforceDecimals {
		r[fieldChartDecimalPlaces] = ch.DecimalPlaces
	}

	if ch.Legend.Type != "" {
		r[fieldChartLegend] = ch.Legend
	}

	if len(ch.FillColumns) > 0 {
		r[fieldChartFillColumns] = ch.FillColumns
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
		fieldChartNoteOnEmpty: ch.NoteOnEmpty,
		fieldChartShade:       ch.Shade,
	})

	assignNonZeroStrings(r, map[string]string{
		fieldChartNote:           ch.Note,
		fieldPrefix:              ch.Prefix,
		fieldSuffix:              ch.Suffix,
		fieldChartGeom:           ch.Geom,
		fieldChartXCol:           ch.XCol,
		fieldChartYCol:           ch.YCol,
		fieldChartPosition:       ch.Position,
		fieldChartTickPrefix:     ch.TickPrefix,
		fieldChartTickSuffix:     ch.TickSuffix,
		fieldChartTimeFormat:     ch.TimeFormat,
		fieldChartHoverDimension: ch.HoverDimension,
	})

	assignNonZeroInts(r, map[string]int{
		fieldChartXPos:     ch.XPos,
		fieldChartYPos:     ch.YPos,
		fieldChartBinCount: ch.BinCount,
		fieldChartBinSize:  ch.BinSize,
	})

	return r
}

func convertAxes(iAxes map[string]influxdb.Axis) axes {
	out := make(axes, 0, len(iAxes))
	for name, a := range iAxes {
		out = append(out, axis{
			Base:   a.Base,
			Label:  a.Label,
			Name:   name,
			Prefix: a.Prefix,
			Scale:  a.Scale,
			Suffix: a.Suffix,
		})
	}
	return out
}

func convertColors(iColors []influxdb.ViewColor) colors {
	out := make(colors, 0, len(iColors))
	for _, ic := range iColors {
		out = append(out, &color{
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
func TaskToObject(name string, t influxdb.Task) Object {
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
