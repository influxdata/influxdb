package pkger

import (
	"errors"
	"regexp"
	"sort"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification"
	icheck "github.com/influxdata/influxdb/notification/check"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/notification/rule"
)

// ResourceToClone is a resource that will be cloned.
type ResourceToClone struct {
	Kind Kind        `json:"kind"`
	ID   influxdb.ID `json:"id"`
	Name string      `json:"name"`
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
		case ok && kr.Name == r.Name:
		case ok && kr.Name != "" && r.Name == "":
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

func bucketToObject(bkt influxdb.Bucket, name string) Object {
	if name == "" {
		name = bkt.Name
	}
	k := Object{
		APIVersion: APIVersion,
		Type:       KindBucket,
		Metadata:   convertToMetadataResource(name),
		Spec:       make(Resource),
	}
	assignNonZeroStrings(k.Spec, map[string]string{fieldDescription: bkt.Description})
	if bkt.RetentionPeriod != 0 {
		k.Spec[fieldBucketRetentionRules] = retentionRules{newRetentionRule(bkt.RetentionPeriod)}
	}
	return k
}

func checkToObject(ch influxdb.Check, name string) Object {
	if name == "" {
		name = ch.GetName()
	}
	k := Object{
		APIVersion: APIVersion,
		Metadata:   convertToMetadataResource(name),
		Spec: Resource{
			fieldStatus: influxdb.TaskStatusActive,
		},
	}
	assignNonZeroStrings(k.Spec, map[string]string{fieldDescription: ch.GetDescription()})

	assignBase := func(base icheck.Base) {
		k.Spec[fieldQuery] = strings.TrimSpace(base.Query.Text)
		k.Spec[fieldCheckStatusMessageTemplate] = base.StatusMessageTemplate
		assignNonZeroFluxDurs(k.Spec, map[string]*notification.Duration{
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
			k.Spec[fieldCheckTags] = tags
		}
	}

	switch cT := ch.(type) {
	case *icheck.Deadman:
		k.Type = KindCheckDeadman
		assignBase(cT.Base)
		assignNonZeroFluxDurs(k.Spec, map[string]*notification.Duration{
			fieldCheckTimeSince: cT.TimeSince,
			fieldCheckStaleTime: cT.StaleTime,
		})
		k.Spec[fieldLevel] = cT.Level.String()
		assignNonZeroBools(k.Spec, map[string]bool{fieldCheckReportZero: cT.ReportZero})
	case *icheck.Threshold:
		k.Type = KindCheckThreshold
		assignBase(cT.Base)
		var thresholds []Resource
		for _, th := range cT.Thresholds {
			thresholds = append(thresholds, convertThreshold(th))
		}
		k.Spec[fieldCheckThresholds] = thresholds
	}
	return k
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
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Position = p.Position
	case influxdb.SingleStatViewProperties:
		setCommon(chartKindSingleStat, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		ch.TickPrefix = p.TickPrefix
		ch.TickSuffix = p.TickSuffix
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
	case influxdb.XYViewProperties:
		setCommon(chartKindXY, p.ViewColors, influxdb.DecimalPlaces{}, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.Shade = p.ShadeBelow
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
		ch.Position = p.Position
	}

	return ch
}

func convertChartToResource(ch chart) Resource {
	r := Resource{
		fieldKind:         ch.Kind.title(),
		fieldName:         ch.Name,
		fieldChartQueries: ch.Queries,
		fieldChartHeight:  ch.Height,
		fieldChartWidth:   ch.Width,
	}
	if len(ch.Colors) > 0 {
		r[fieldChartColors] = ch.Colors
	}
	if len(ch.Axes) > 0 {
		r[fieldChartAxes] = ch.Axes
	}
	if ch.EnforceDecimals {
		r[fieldChartDecimalPlaces] = ch.DecimalPlaces
	}

	if ch.Legend.Type != "" {
		r[fieldChartLegend] = ch.Legend
	}

	if zero := new(tableOptions); ch.TableOptions != *zero {
		tRes := make(Resource)
		assignNonZeroBools(tRes, map[string]bool{
			fieldChartTableOptionVerticalTimeAxis: ch.TableOptions.VerticalTimeAxis,
			fieldChartTableOptionFixFirstColumn:   ch.TableOptions.VerticalTimeAxis,
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
		fieldChartNote:       ch.Note,
		fieldPrefix:          ch.Prefix,
		fieldSuffix:          ch.Suffix,
		fieldChartGeom:       ch.Geom,
		fieldChartXCol:       ch.XCol,
		fieldChartYCol:       ch.YCol,
		fieldChartPosition:   ch.Position,
		fieldChartTickPrefix: ch.TickPrefix,
		fieldChartTickSuffix: ch.TickSuffix,
		fieldChartTimeFormat: ch.TimeFormat,
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

// DashboardToObject converts an influxdb.Dashboard to a pkger.Resource.
func DashboardToObject(dash influxdb.Dashboard, name string) Object {
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
		if cell.ID == influxdb.ID(0) {
			continue
		}
		ch := convertCellView(*cell)
		if !ch.Kind.ok() {
			continue
		}
		charts = append(charts, convertChartToResource(ch))
	}

	return Object{
		APIVersion: APIVersion,
		Type:       KindDashboard,
		Metadata:   convertToMetadataResource(name),
		Spec: Resource{
			fieldDescription: dash.Description,
			fieldDashCharts:  charts,
		},
	}
}

func labelToObject(l influxdb.Label, name string) Object {
	if name == "" {
		name = l.Name
	}
	k := Object{
		APIVersion: APIVersion,
		Type:       KindLabel,
		Metadata:   convertToMetadataResource(name),
		Spec:       make(Resource),
	}

	assignNonZeroStrings(k.Spec, map[string]string{
		fieldDescription: l.Properties["description"],
		fieldLabelColor:  l.Properties["color"],
	})
	return k
}

func endpointKind(e influxdb.NotificationEndpoint, name string) Object {
	if name == "" {
		name = e.GetName()
	}
	k := Object{
		APIVersion: APIVersion,
		Metadata:   convertToMetadataResource(name),
		Spec:       make(Resource),
	}
	assignNonZeroStrings(k.Spec, map[string]string{
		fieldDescription: e.GetDescription(),
		fieldStatus:      string(e.GetStatus()),
	})

	switch actual := e.(type) {
	case *endpoint.HTTP:
		k.Type = KindNotificationEndpointHTTP
		k.Spec[fieldNotificationEndpointHTTPMethod] = actual.Method
		k.Spec[fieldNotificationEndpointURL] = actual.URL
		k.Spec[fieldType] = actual.AuthMethod
		assignNonZeroSecrets(k.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointPassword: actual.Password,
			fieldNotificationEndpointToken:    actual.Token,
			fieldNotificationEndpointUsername: actual.Username,
		})
	case *endpoint.PagerDuty:
		k.Type = KindNotificationEndpointPagerDuty
		k.Spec[fieldNotificationEndpointURL] = actual.ClientURL
		assignNonZeroSecrets(k.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointRoutingKey: actual.RoutingKey,
		})
	case *endpoint.Slack:
		k.Type = KindNotificationEndpointSlack
		k.Spec[fieldNotificationEndpointURL] = actual.URL
		assignNonZeroSecrets(k.Spec, map[string]influxdb.SecretField{
			fieldNotificationEndpointToken: actual.Token,
		})
	}

	return k
}

func ruleToObject(iRule influxdb.NotificationRule, endpointName, name string) Object {
	if name == "" {
		name = iRule.GetName()
	}
	k := Object{
		APIVersion: APIVersion,
		Type:       KindNotificationRule,
		Metadata:   convertToMetadataResource(name),
		Spec: Resource{
			fieldNotificationRuleEndpointName: endpointName,
		},
	}
	assignNonZeroStrings(k.Spec, map[string]string{
		fieldDescription: iRule.GetDescription(),
	})

	assignBase := func(base rule.Base) {
		assignNonZeroFluxDurs(k.Spec, map[string]*notification.Duration{
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
			k.Spec[fieldNotificationRuleTagRules] = tagRes
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
			k.Spec[fieldNotificationRuleStatusRules] = statusRuleRes
		}
	}

	switch t := iRule.(type) {
	case *rule.HTTP:
		assignBase(t.Base)
	case *rule.PagerDuty:
		assignBase(t.Base)
		k.Spec[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
	case *rule.Slack:
		assignBase(t.Base)
		k.Spec[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
		assignNonZeroStrings(k.Spec, map[string]string{fieldNotificationRuleChannel: t.Channel})
	}

	return k
}

// regex used to rip out the hard coded task option stuffs
var taskFluxRegex = regexp.MustCompile(`option task = {(.|\n)*?}`)

func taskToObject(t influxdb.Task, name string) Object {
	if name == "" {
		name = t.Name
	}

	query := strings.TrimSpace(taskFluxRegex.ReplaceAllString(t.Flux, ""))

	k := Object{
		APIVersion: APIVersion,
		Type:       KindTask,
		Metadata:   convertToMetadataResource(name),
		Spec: Resource{
			fieldQuery: strings.TrimSpace(query),
		},
	}
	assignNonZeroStrings(k.Spec, map[string]string{
		fieldTaskCron:    t.Cron,
		fieldDescription: t.Description,
		fieldEvery:       t.Every,
		fieldOffset:      durToStr(t.Offset),
	})
	return k
}

func telegrafToObject(t influxdb.TelegrafConfig, name string) Object {
	if name == "" {
		name = t.Name
	}
	k := Object{
		APIVersion: APIVersion,
		Type:       KindTelegraf,
		Metadata:   convertToMetadataResource(name),
		Spec: Resource{
			fieldTelegrafConfig: t.Config,
		},
	}
	assignNonZeroStrings(k.Spec, map[string]string{
		fieldDescription: t.Description,
	})
	return k
}

// VariableToObject converts an influxdb.Variable to a pkger.Object.
func VariableToObject(v influxdb.Variable, name string) Object {
	if name == "" {
		name = v.Name
	}

	k := Object{
		APIVersion: APIVersion,
		Type:       KindVariable,
		Metadata:   convertToMetadataResource(name),
		Spec:       make(Resource),
	}
	assignNonZeroStrings(k.Spec, map[string]string{fieldDescription: v.Description})

	args := v.Arguments
	if args == nil {
		return k
	}
	k.Spec[fieldType] = args.Type

	switch args.Type {
	case fieldArgTypeConstant:
		vals, ok := args.Values.(influxdb.VariableConstantValues)
		if ok {
			k.Spec[fieldValues] = []string(vals)
		}
	case fieldArgTypeMap:
		vals, ok := args.Values.(influxdb.VariableMapValues)
		if ok {
			k.Spec[fieldValues] = map[string]string(vals)
		}
	case fieldArgTypeQuery:
		vals, ok := args.Values.(influxdb.VariableQueryValues)
		if ok {
			k.Spec[fieldLanguage] = vals.Language
			k.Spec[fieldQuery] = strings.TrimSpace(vals.Query)
		}
	}

	return k
}

func convertToMetadataResource(name string) Resource {
	return Resource{
		"name": name,
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
