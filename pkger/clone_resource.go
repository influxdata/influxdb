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

func bucketToResource(bkt influxdb.Bucket, name string) Resource {
	if name == "" {
		name = bkt.Name
	}
	r := Resource{
		fieldKind: KindBucket.title(),
		fieldName: name,
	}
	assignNonZeroStrings(r, map[string]string{fieldDescription: bkt.Description})
	if bkt.RetentionPeriod != 0 {
		r[fieldBucketRetentionRules] = retentionRules{newRetentionRule(bkt.RetentionPeriod)}
	}
	return r
}

func checkToResource(ch influxdb.Check, name string) Resource {
	if name == "" {
		name = ch.GetName()
	}
	r := Resource{
		fieldName:   name,
		fieldStatus: string(influxdb.TaskStatusActive),
	}
	assignNonZeroStrings(r, map[string]string{fieldDescription: ch.GetDescription()})

	assignBase := func(base icheck.Base) {
		r[fieldQuery] = base.Query.Text
		r[fieldCheckStatusMessageTemplate] = base.StatusMessageTemplate
		assignNonZeroFluxDurs(r, map[string]*notification.Duration{
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
			r[fieldCheckTags] = tags
		}
	}

	switch cT := ch.(type) {
	case *icheck.Deadman:
		r[fieldKind] = KindCheckDeadman.title()
		assignBase(cT.Base)
		assignNonZeroFluxDurs(r, map[string]*notification.Duration{
			fieldCheckTimeSince: cT.TimeSince,
			fieldCheckStaleTime: cT.StaleTime,
		})
		r[fieldLevel] = cT.Level.String()
		assignNonZeroBools(r, map[string]bool{fieldCheckReportZero: cT.ReportZero})
	case *icheck.Threshold:
		r[fieldKind] = KindCheckThreshold.title()
		assignBase(cT.Base)
		var thresholds []Resource
		for _, th := range cT.Thresholds {
			thresholds = append(thresholds, convertThreshold(th))
		}
		r[fieldCheckThresholds] = thresholds
	}
	return r
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

	assignNonZeroBools(r, map[string]bool{
		fieldChartNoteOnEmpty: ch.NoteOnEmpty,
		fieldChartShade:       ch.Shade,
	})

	assignNonZeroStrings(r, map[string]string{
		fieldChartNote:     ch.Note,
		fieldPrefix:        ch.Prefix,
		fieldSuffix:        ch.Suffix,
		fieldChartGeom:     ch.Geom,
		fieldChartXCol:     ch.XCol,
		fieldChartYCol:     ch.YCol,
		fieldChartPosition: ch.Position,
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
		out = append(out, query{Query: iq.Text})
	}
	return out
}

func dashboardToResource(dash influxdb.Dashboard, name string) Resource {
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

	return Resource{
		fieldKind:        KindDashboard.title(),
		fieldName:        name,
		fieldDescription: dash.Description,
		fieldDashCharts:  charts,
	}
}

func labelToResource(l influxdb.Label, name string) Resource {
	if name == "" {
		name = l.Name
	}
	r := Resource{
		fieldKind: KindLabel.title(),
		fieldName: name,
	}

	assignNonZeroStrings(r, map[string]string{
		fieldDescription: l.Properties["description"],
		fieldLabelColor:  l.Properties["color"],
	})
	return r
}

func endpointToResource(e influxdb.NotificationEndpoint, name string) Resource {
	if name == "" {
		name = e.GetName()
	}
	r := Resource{
		fieldName: name,
	}
	assignNonZeroStrings(r, map[string]string{
		fieldDescription: e.GetDescription(),
		fieldStatus:      string(e.GetStatus()),
	})

	switch actual := e.(type) {
	case *endpoint.HTTP:
		r[fieldKind] = KindNotificationEndpointHTTP.title()
		r[fieldNotificationEndpointHTTPMethod] = actual.Method
		r[fieldNotificationEndpointURL] = actual.URL
		r[fieldType] = actual.AuthMethod
		assignNonZeroSecrets(r, map[string]influxdb.SecretField{
			fieldNotificationEndpointPassword: actual.Password,
			fieldNotificationEndpointToken:    actual.Token,
			fieldNotificationEndpointUsername: actual.Username,
		})
	case *endpoint.PagerDuty:
		r[fieldKind] = KindNotificationEndpointPagerDuty.title()
		r[fieldNotificationEndpointURL] = actual.ClientURL
		assignNonZeroSecrets(r, map[string]influxdb.SecretField{
			fieldNotificationEndpointRoutingKey: actual.RoutingKey,
		})
	case *endpoint.Slack:
		r[fieldKind] = KindNotificationEndpointSlack.title()
		r[fieldNotificationEndpointURL] = actual.URL
		assignNonZeroSecrets(r, map[string]influxdb.SecretField{
			fieldNotificationEndpointToken: actual.Token,
		})
	}

	return r
}

func ruleToResource(iRule influxdb.NotificationRule, endpointName, name string) Resource {
	if name == "" {
		name = iRule.GetName()
	}
	r := Resource{
		fieldKind:                         KindNotificationRule.title(),
		fieldName:                         name,
		fieldNotificationRuleEndpointName: endpointName,
	}
	assignNonZeroStrings(r, map[string]string{
		fieldDescription: iRule.GetDescription(),
	})

	assignBase := func(base rule.Base) {
		assignNonZeroFluxDurs(r, map[string]*notification.Duration{
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
			r[fieldNotificationRuleTagRules] = tagRes
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
			r[fieldNotificationRuleStatusRules] = statusRuleRes
		}
	}

	switch t := iRule.(type) {
	case *rule.HTTP:
		assignBase(t.Base)
	case *rule.PagerDuty:
		assignBase(t.Base)
		r[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
	case *rule.Slack:
		assignBase(t.Base)
		r[fieldNotificationRuleMessageTemplate] = t.MessageTemplate
		assignNonZeroStrings(r, map[string]string{fieldNotificationRuleChannel: t.Channel})
	}

	return r
}

// regex used to rip out the hard coded task option stuffs
var taskFluxRegex = regexp.MustCompile(`option task = {(.|\n)*?}`)

func taskToResource(t influxdb.Task, name string) Resource {
	if name == "" {
		name = t.Name
	}

	query := strings.TrimSpace(taskFluxRegex.ReplaceAllString(t.Flux, ""))

	r := Resource{
		fieldKind:  KindTask.title(),
		fieldName:  name,
		fieldQuery: query,
	}
	assignNonZeroStrings(r, map[string]string{
		fieldTaskCron:    t.Cron,
		fieldDescription: t.Description,
		fieldEvery:       t.Every,
		fieldOffset:      durToStr(t.Offset),
	})
	return r
}

func telegrafToResource(t influxdb.TelegrafConfig, name string) Resource {
	if name == "" {
		name = t.Name
	}
	r := Resource{
		fieldKind:           KindTelegraf.title(),
		fieldName:           name,
		fieldTelegrafConfig: t.Config,
	}
	assignNonZeroStrings(r, map[string]string{
		fieldDescription: t.Description,
	})
	return r
}

func variableToResource(v influxdb.Variable, name string) Resource {
	if name == "" {
		name = v.Name
	}

	r := Resource{
		fieldKind: KindVariable.title(),
		fieldName: name,
	}
	assignNonZeroStrings(r, map[string]string{fieldDescription: v.Description})

	args := v.Arguments
	if args == nil {
		return r
	}
	r[fieldType] = args.Type

	switch args.Type {
	case fieldArgTypeConstant:
		vals, ok := args.Values.(influxdb.VariableConstantValues)
		if ok {
			r[fieldValues] = []string(vals)
		}
	case fieldArgTypeMap:
		vals, ok := args.Values.(influxdb.VariableMapValues)
		if ok {
			r[fieldValues] = map[string]string(vals)
		}
	case fieldArgTypeQuery:
		vals, ok := args.Values.(influxdb.VariableQueryValues)
		if ok {
			r[fieldLanguage] = vals.Language
			r[fieldQuery] = vals.Query
		}
	}

	return r
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
