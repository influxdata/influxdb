package pkger

import (
	"errors"
	"sort"

	"github.com/influxdata/influxdb"
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

func bucketToResource(bkt influxdb.Bucket, name string) Resource {
	if name == "" {
		name = bkt.Name
	}
	return Resource{
		fieldKind:                  KindBucket.title(),
		fieldName:                  name,
		fieldDescription:           bkt.Description,
		fieldBucketRetentionPeriod: bkt.RetentionPeriod.String(),
	}
}

type cellView struct {
	c influxdb.Cell
	v influxdb.View
}

func convertCellView(cv cellView) chart {
	ch := chart{
		Name:   cv.v.Name,
		Height: int(cv.c.H),
		Width:  int(cv.c.W),
		XPos:   int(cv.c.X),
		YPos:   int(cv.c.Y),
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

	props := cv.v.Properties
	switch p := props.(type) {
	case influxdb.GaugeViewProperties:
		setCommon(chartKindGauge, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
	case influxdb.LinePlusSingleStatProperties:
		setCommon(chartKindSingleStatPlusLine, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Shade = p.ShadeBelow
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
	case influxdb.SingleStatViewProperties:
		setCommon(chartKindSingleStat, p.ViewColors, p.DecimalPlaces, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, p.Prefix, p.Suffix)
	case influxdb.XYViewProperties:
		setCommon(chartKindXY, p.ViewColors, influxdb.DecimalPlaces{}, p.Queries)
		setNoteFixes(p.Note, p.ShowNoteWhenEmpty, "", "")
		setLegend(p.Legend)
		ch.Axes = convertAxes(p.Axes)
		ch.Geom = p.Geom
		ch.Shade = p.ShadeBelow
		ch.XCol = p.XColumn
		ch.YCol = p.YColumn
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

	ignoreFalseBools := map[string]bool{
		fieldChartNoteOnEmpty: ch.NoteOnEmpty,
		fieldChartShade:       ch.Shade,
	}
	for k, v := range ignoreFalseBools {
		if v {
			r[k] = v
		}
	}

	ignoreEmptyStrPairs := map[string]string{
		fieldChartNote: ch.Note,
		fieldPrefix:    ch.Prefix,
		fieldSuffix:    ch.Suffix,
		fieldChartGeom: ch.Geom,
		fieldChartXCol: ch.XCol,
		fieldChartYCol: ch.YCol,
	}
	for k, v := range ignoreEmptyStrPairs {
		if v != "" {
			r[k] = v
		}
	}

	ignoreEmptyIntPairs := map[string]int{
		fieldChartXPos: ch.XPos,
		fieldChartYPos: ch.YPos,
	}
	for k, v := range ignoreEmptyIntPairs {
		if v != 0 {
			r[k] = v
		}
	}

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

func dashboardToResource(dash influxdb.Dashboard, cellViews []cellView, name string) Resource {
	if name == "" {
		name = dash.Name
	}

	sort.Slice(cellViews, func(i, j int) bool {
		ic, jc := cellViews[i].c, cellViews[j].c
		if ic.X == jc.X {
			return ic.Y < jc.Y
		}
		return ic.X < jc.X
	})

	charts := make([]Resource, 0, len(cellViews))
	for _, cv := range cellViews {
		if cv.c.ID == influxdb.ID(0) {
			continue
		}
		ch := convertCellView(cv)
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
	return Resource{
		fieldKind:        KindLabel.title(),
		fieldName:        name,
		fieldLabelColor:  l.Properties["color"],
		fieldDescription: l.Properties["description"],
	}
}

func variableToResource(v influxdb.Variable, name string) Resource {
	if name == "" {
		name = v.Name
	}

	r := Resource{
		fieldKind:        KindVariable.title(),
		fieldName:        name,
		fieldDescription: v.Description,
	}
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
			r[fieldVarLanguage] = vals.Language
			r[fieldQuery] = vals.Query
		}
	}

	return r
}
