package pkger

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

type identity struct {
	name         *references
	displayName  *references
	shouldRemove bool
}

func (i *identity) Name() string {
	if displayName := i.displayName.String(); displayName != "" {
		return displayName
	}
	return i.name.String()
}

func (i *identity) PkgName() string {
	return i.name.String()
}

const (
	fieldAPIVersion   = "apiVersion"
	fieldAssociations = "associations"
	fieldDescription  = "description"
	fieldEvery        = "every"
	fieldKey          = "key"
	fieldKind         = "kind"
	fieldLanguage     = "language"
	fieldLevel        = "level"
	fieldMin          = "min"
	fieldMax          = "max"
	fieldMetadata     = "metadata"
	fieldName         = "name"
	fieldOffset       = "offset"
	fieldOperator     = "operator"
	fieldPrefix       = "prefix"
	fieldQuery        = "query"
	fieldSuffix       = "suffix"
	fieldSpec         = "spec"
	fieldStatus       = "status"
	fieldType         = "type"
	fieldValue        = "value"
	fieldValues       = "values"
)

const (
	fieldBucketRetentionRules = "retentionRules"
)

const bucketNameMinLength = 2

type bucket struct {
	identity

	Description    string
	RetentionRules retentionRules
	labels         sortedLabels
}

func (b *bucket) summarize() SummaryBucket {
	return SummaryBucket{
		Name:              b.Name(),
		PkgName:           b.PkgName(),
		Description:       b.Description,
		RetentionPeriod:   b.RetentionRules.RP(),
		LabelAssociations: toSummaryLabels(b.labels...),
	}
}

func (b *bucket) ResourceType() influxdb.ResourceType {
	return KindBucket.ResourceType()
}

func (b *bucket) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(b.Name(), bucketNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	vErrs = append(vErrs, b.RetentionRules.valid()...)
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

const (
	retentionRuleTypeExpire = "expire"
)

type retentionRule struct {
	Type    string `json:"type" yaml:"type"`
	Seconds int    `json:"everySeconds" yaml:"everySeconds"`
}

func newRetentionRule(d time.Duration) retentionRule {
	return retentionRule{
		Type:    retentionRuleTypeExpire,
		Seconds: int(d.Round(time.Second) / time.Second),
	}
}

func (r retentionRule) valid() []validationErr {
	const hour = 3600
	var ff []validationErr
	if r.Seconds < hour {
		ff = append(ff, validationErr{
			Field: fieldRetentionRulesEverySeconds,
			Msg:   "seconds must be a minimum of " + strconv.Itoa(hour),
		})
	}
	if r.Type != retentionRuleTypeExpire {
		ff = append(ff, validationErr{
			Field: fieldType,
			Msg:   `type must be "expire"`,
		})
	}
	return ff
}

const (
	fieldRetentionRulesEverySeconds = "everySeconds"
)

type retentionRules []retentionRule

func (r retentionRules) RP() time.Duration {
	// TODO: this feels very odd to me, will need to follow up with
	//  team to better understand this
	for _, rule := range r {
		return time.Duration(rule.Seconds) * time.Second
	}
	return 0
}

func (r retentionRules) valid() []validationErr {
	var failures []validationErr
	for i, rule := range r {
		if ff := rule.valid(); len(ff) > 0 {
			failures = append(failures, validationErr{
				Field:  fieldBucketRetentionRules,
				Index:  intPtr(i),
				Nested: ff,
			})
		}
	}
	return failures
}

type checkKind int

const (
	checkKindDeadman checkKind = iota + 1
	checkKindThreshold
)

const (
	fieldCheckAllValues             = "allValues"
	fieldCheckReportZero            = "reportZero"
	fieldCheckStaleTime             = "staleTime"
	fieldCheckStatusMessageTemplate = "statusMessageTemplate"
	fieldCheckTags                  = "tags"
	fieldCheckThresholds            = "thresholds"
	fieldCheckTimeSince             = "timeSince"
)

const checkNameMinLength = 1

type check struct {
	identity

	kind          checkKind
	description   string
	every         time.Duration
	level         string
	offset        time.Duration
	query         string
	reportZero    bool
	staleTime     time.Duration
	status        string
	statusMessage string
	tags          []struct{ k, v string }
	timeSince     time.Duration
	thresholds    []threshold

	labels sortedLabels
}

func (c *check) Labels() []*label {
	return c.labels
}

func (c *check) ResourceType() influxdb.ResourceType {
	return KindCheck.ResourceType()
}

func (c *check) Status() influxdb.Status {
	status := influxdb.Status(c.status)
	if status == "" {
		status = influxdb.Active
	}
	return status
}

func (c *check) summarize() SummaryCheck {
	base := icheck.Base{
		Name:                  c.Name(),
		Description:           c.description,
		Every:                 toNotificationDuration(c.every),
		Offset:                toNotificationDuration(c.offset),
		StatusMessageTemplate: c.statusMessage,
	}
	base.Query.Text = c.query
	for _, tag := range c.tags {
		base.Tags = append(base.Tags, influxdb.Tag{Key: tag.k, Value: tag.v})
	}

	sum := SummaryCheck{
		PkgName:           c.PkgName(),
		Status:            c.Status(),
		LabelAssociations: toSummaryLabels(c.labels...),
	}
	switch c.kind {
	case checkKindThreshold:
		sum.Check = &icheck.Threshold{
			Base:       base,
			Thresholds: toInfluxThresholds(c.thresholds...),
		}
	case checkKindDeadman:
		sum.Check = &icheck.Deadman{
			Base:       base,
			Level:      notification.ParseCheckLevel(strings.ToUpper(c.level)),
			ReportZero: c.reportZero,
			StaleTime:  toNotificationDuration(c.staleTime),
			TimeSince:  toNotificationDuration(c.timeSince),
		}
	}
	return sum
}

func (c *check) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(c.Name(), checkNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if c.every == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldEvery,
			Msg:   "duration value must be provided that is >= 5s (seconds)",
		})
	}
	if c.query == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldQuery,
			Msg:   "must provide a non zero value",
		})
	}
	if c.statusMessage == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldCheckStatusMessageTemplate,
			Msg:   `must provide a template; ex. "Check: ${ r._check_name } is: ${ r._level }"`,
		})
	}
	if status := c.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   "must be 1 of [active, inactive]",
		})
	}

	switch c.kind {
	case checkKindThreshold:
		if len(c.thresholds) == 0 {
			vErrs = append(vErrs, validationErr{
				Field: fieldCheckThresholds,
				Msg:   "must provide at least 1 threshold entry",
			})
		}
		for i, th := range c.thresholds {
			for _, fail := range th.valid() {
				fail.Index = intPtr(i)
				vErrs = append(vErrs, fail)
			}
		}
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

type thresholdType string

const (
	thresholdTypeGreater      thresholdType = "greater"
	thresholdTypeLesser       thresholdType = "lesser"
	thresholdTypeInsideRange  thresholdType = "inside_range"
	thresholdTypeOutsideRange thresholdType = "outside_range"
)

var thresholdTypes = map[thresholdType]bool{
	thresholdTypeGreater:      true,
	thresholdTypeLesser:       true,
	thresholdTypeInsideRange:  true,
	thresholdTypeOutsideRange: true,
}

type threshold struct {
	threshType thresholdType
	allVals    bool
	level      string
	val        float64
	min, max   float64
}

func (t threshold) valid() []validationErr {
	var vErrs []validationErr
	if notification.ParseCheckLevel(t.level) == notification.Unknown {
		vErrs = append(vErrs, validationErr{
			Field: fieldLevel,
			Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", t.level),
		})
	}
	if !thresholdTypes[t.threshType] {
		vErrs = append(vErrs, validationErr{
			Field: fieldType,
			Msg:   fmt.Sprintf("must be 1 in [Lesser, Greater, Inside_Range, Outside_Range]; got=%q", t.threshType),
		})
	}
	if t.min > t.max {
		vErrs = append(vErrs, validationErr{
			Field: fieldMin,
			Msg:   "min must be < max",
		})
	}
	return vErrs
}

func toInfluxThresholds(thresholds ...threshold) []icheck.ThresholdConfig {
	var iThresh []icheck.ThresholdConfig
	for _, th := range thresholds {
		base := icheck.ThresholdConfigBase{
			AllValues: th.allVals,
			Level:     notification.ParseCheckLevel(th.level),
		}
		switch th.threshType {
		case thresholdTypeGreater:
			iThresh = append(iThresh, icheck.Greater{
				ThresholdConfigBase: base,
				Value:               th.val,
			})
		case thresholdTypeLesser:
			iThresh = append(iThresh, icheck.Lesser{
				ThresholdConfigBase: base,
				Value:               th.val,
			})
		case thresholdTypeInsideRange, thresholdTypeOutsideRange:
			iThresh = append(iThresh, icheck.Range{
				ThresholdConfigBase: base,
				Max:                 th.max,
				Min:                 th.min,
				Within:              th.threshType == thresholdTypeInsideRange,
			})
		}
	}
	return iThresh
}

// chartKind identifies what kind of chart is eluded too. Each
// chart kind has their own requirements for what constitutes
// a chart.
type chartKind string

// available chart kinds
const (
	chartKindUnknown            chartKind = ""
	chartKindGauge              chartKind = "gauge"
	chartKindHeatMap            chartKind = "heatmap"
	chartKindHistogram          chartKind = "histogram"
	chartKindMarkdown           chartKind = "markdown"
	chartKindScatter            chartKind = "scatter"
	chartKindSingleStat         chartKind = "single_stat"
	chartKindSingleStatPlusLine chartKind = "single_stat_plus_line"
	chartKindTable              chartKind = "table"
	chartKindXY                 chartKind = "xy"
)

func (c chartKind) ok() bool {
	switch c {
	case chartKindGauge, chartKindHeatMap, chartKindHistogram,
		chartKindMarkdown, chartKindScatter, chartKindSingleStat,
		chartKindSingleStatPlusLine, chartKindTable, chartKindXY:
		return true
	default:
		return false
	}
}

func (c chartKind) title() string {
	spacedKind := strings.ReplaceAll(string(c), "_", " ")
	return strings.ReplaceAll(strings.Title(spacedKind), " ", "_")
}

const (
	fieldDashCharts = "charts"
)

const dashboardNameMinLength = 2

type dashboard struct {
	identity

	Description string
	Charts      []chart

	labels sortedLabels
}

func (d *dashboard) Labels() []*label {
	return d.labels
}

func (d *dashboard) ResourceType() influxdb.ResourceType {
	return KindDashboard.ResourceType()
}

func (d *dashboard) summarize() SummaryDashboard {
	iDash := SummaryDashboard{
		PkgName:           d.PkgName(),
		Name:              d.Name(),
		Description:       d.Description,
		LabelAssociations: toSummaryLabels(d.labels...),
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

func (d *dashboard) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(d.Name(), dashboardNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

const (
	fieldChartAxes          = "axes"
	fieldChartBinCount      = "binCount"
	fieldChartBinSize       = "binSize"
	fieldChartColors        = "colors"
	fieldChartDecimalPlaces = "decimalPlaces"
	fieldChartDomain        = "domain"
	fieldChartFillColumns   = "fillColumns"
	fieldChartGeom          = "geom"
	fieldChartHeight        = "height"
	fieldChartLegend        = "legend"
	fieldChartNote          = "note"
	fieldChartNoteOnEmpty   = "noteOnEmpty"
	fieldChartPosition      = "position"
	fieldChartQueries       = "queries"
	fieldChartShade         = "shade"
	fieldChartFieldOptions  = "fieldOptions"
	fieldChartTableOptions  = "tableOptions"
	fieldChartTickPrefix    = "tickPrefix"
	fieldChartTickSuffix    = "tickSuffix"
	fieldChartTimeFormat    = "timeFormat"
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
	TickPrefix      string
	Suffix          string
	TickSuffix      string
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
	XCol, YCol      string
	XPos, YPos      int
	Height, Width   int
	BinSize         int
	BinCount        int
	Position        string
	FieldOptions    []fieldOption
	FillColumns     []string
	TableOptions    tableOptions
	TimeFormat      string
}

func (c chart) properties() influxdb.ViewProperties {
	switch c.Kind {
	case chartKindGauge:
		return influxdb.GaugeViewProperties{
			Type:       influxdb.ViewPropertyTypeGauge,
			Queries:    c.Queries.influxDashQueries(),
			Prefix:     c.Prefix,
			TickPrefix: c.TickPrefix,
			Suffix:     c.Suffix,
			TickSuffix: c.TickSuffix,
			ViewColors: c.Colors.influxViewColors(),
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
		}
	case chartKindHeatMap:
		return influxdb.HeatmapViewProperties{
			Type:              influxdb.ViewPropertyTypeHeatMap,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.strings(),
			BinSize:           int32(c.BinSize),
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			XDomain:           c.Axes.get("x").Domain,
			YDomain:           c.Axes.get("y").Domain,
			XPrefix:           c.Axes.get("x").Prefix,
			YPrefix:           c.Axes.get("y").Prefix,
			XSuffix:           c.Axes.get("x").Suffix,
			YSuffix:           c.Axes.get("y").Suffix,
			XAxisLabel:        c.Axes.get("x").Label,
			YAxisLabel:        c.Axes.get("y").Label,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			TimeFormat:        c.TimeFormat,
		}
	case chartKindHistogram:
		return influxdb.HistogramViewProperties{
			Type:              influxdb.ViewPropertyTypeHistogram,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.influxViewColors(),
			FillColumns:       c.FillColumns,
			XColumn:           c.XCol,
			XDomain:           c.Axes.get("x").Domain,
			XAxisLabel:        c.Axes.get("x").Label,
			Position:          c.Position,
			BinCount:          c.BinCount,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
		}
	case chartKindMarkdown:
		return influxdb.MarkdownViewProperties{
			Type: influxdb.ViewPropertyTypeMarkdown,
			Note: c.Note,
		}
	case chartKindScatter:
		return influxdb.ScatterViewProperties{
			Type:              influxdb.ViewPropertyTypeScatter,
			Queries:           c.Queries.influxDashQueries(),
			ViewColors:        c.Colors.strings(),
			XColumn:           c.XCol,
			YColumn:           c.YCol,
			XDomain:           c.Axes.get("x").Domain,
			YDomain:           c.Axes.get("y").Domain,
			XPrefix:           c.Axes.get("x").Prefix,
			YPrefix:           c.Axes.get("y").Prefix,
			XSuffix:           c.Axes.get("x").Suffix,
			YSuffix:           c.Axes.get("y").Suffix,
			XAxisLabel:        c.Axes.get("x").Label,
			YAxisLabel:        c.Axes.get("y").Label,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			TimeFormat:        c.TimeFormat,
		}
	case chartKindSingleStat:
		return influxdb.SingleStatViewProperties{
			Type:       influxdb.ViewPropertyTypeSingleStat,
			Prefix:     c.Prefix,
			TickPrefix: c.TickPrefix,
			Suffix:     c.Suffix,
			TickSuffix: c.TickSuffix,
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
			Position:          c.Position,
		}
	case chartKindTable:
		fieldOptions := make([]influxdb.RenamableField, 0, len(c.FieldOptions))
		for _, fieldOpt := range c.FieldOptions {
			fieldOptions = append(fieldOptions, influxdb.RenamableField{
				InternalName: fieldOpt.FieldName,
				DisplayName:  fieldOpt.DisplayName,
				Visible:      fieldOpt.Visible,
			})
		}

		return influxdb.TableViewProperties{
			Type:              influxdb.ViewPropertyTypeTable,
			Note:              c.Note,
			ShowNoteWhenEmpty: c.NoteOnEmpty,
			DecimalPlaces: influxdb.DecimalPlaces{
				IsEnforced: c.EnforceDecimals,
				Digits:     int32(c.DecimalPlaces),
			},
			Queries:    c.Queries.influxDashQueries(),
			ViewColors: c.Colors.influxViewColors(),
			TableOptions: influxdb.TableOptions{
				VerticalTimeAxis: c.TableOptions.VerticalTimeAxis,
				SortBy: influxdb.RenamableField{
					InternalName: c.TableOptions.SortByField,
				},
				Wrapping:       c.TableOptions.Wrapping,
				FixFirstColumn: c.TableOptions.FixFirstColumn,
			},
			FieldOptions: fieldOptions,
			TimeFormat:   c.TimeFormat,
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
			Position:          c.Position,
			TimeFormat:        c.TimeFormat,
		}
	default:
		return nil
	}
}

func (c chart) validProperties() []validationErr {
	if c.Kind == chartKindMarkdown {
		// at the time of writing, there's nothing to validate for markdown types
		return nil
	}

	var fails []validationErr

	validatorFns := []func() []validationErr{
		c.validBaseProps,
		c.Colors.valid,
	}
	for _, validatorFn := range validatorFns {
		fails = append(fails, validatorFn()...)
	}

	// chart kind specific validations
	switch c.Kind {
	case chartKindGauge:
		fails = append(fails, c.Colors.hasTypes(colorTypeMin, colorTypeMax)...)
	case chartKindHeatMap:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindHistogram:
		fails = append(fails, c.Axes.hasAxes("x")...)
	case chartKindScatter:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
	case chartKindSingleStat:
	case chartKindSingleStatPlusLine:
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
		fails = append(fails, validPosition(c.Position)...)
	case chartKindTable:
		fails = append(fails, validTableOptions(c.TableOptions)...)
	case chartKindXY:
		fails = append(fails, validGeometry(c.Geom)...)
		fails = append(fails, c.Axes.hasAxes("x", "y")...)
		fails = append(fails, validPosition(c.Position)...)
	}

	return fails
}

func validPosition(pos string) []validationErr {
	pos = strings.ToLower(pos)
	if pos != "" && pos != "overlaid" && pos != "stacked" {
		return []validationErr{{
			Field: fieldChartPosition,
			Msg:   fmt.Sprintf("invalid position supplied %q; valid positions is one of [overlaid, stacked]", pos),
		}}
	}
	return nil
}

var geometryTypes = map[string]bool{
	"line":      true,
	"step":      true,
	"stacked":   true,
	"monotoneX": true,
	"bar":       true,
}

func validGeometry(geom string) []validationErr {
	if !geometryTypes[geom] {
		msg := "type not found"
		if geom != "" {
			msg = "type provided is not supported"
		}
		return []validationErr{{
			Field: fieldChartGeom,
			Msg:   fmt.Sprintf("%s: %q", msg, geom),
		}}
	}

	return nil
}

func (c chart) validBaseProps() []validationErr {
	var fails []validationErr
	if c.Width <= 0 {
		fails = append(fails, validationErr{
			Field: fieldChartWidth,
			Msg:   "must be greater than 0",
		})
	}

	if c.Height <= 0 {
		fails = append(fails, validationErr{
			Field: fieldChartHeight,
			Msg:   "must be greater than 0",
		})
	}
	return fails
}

const (
	fieldChartFieldOptionDisplayName = "displayName"
	fieldChartFieldOptionFieldName   = "fieldName"
	fieldChartFieldOptionVisible     = "visible"
)

type fieldOption struct {
	FieldName   string
	DisplayName string
	Visible     bool
}

const (
	fieldChartTableOptionVerticalTimeAxis = "verticalTimeAxis"
	fieldChartTableOptionSortBy           = "sortBy"
	fieldChartTableOptionWrapping         = "wrapping"
	fieldChartTableOptionFixFirstColumn   = "fixFirstColumn"
)

type tableOptions struct {
	VerticalTimeAxis bool
	SortByField      string
	Wrapping         string
	FixFirstColumn   bool
}

func validTableOptions(opts tableOptions) []validationErr {
	var fails []validationErr

	switch opts.Wrapping {
	case "", "single-line", "truncate", "wrap":
	default:
		fails = append(fails, validationErr{
			Field: fieldChartTableOptionWrapping,
			Msg:   `chart table option should 1 in ["single-line", "truncate", "wrap"]`,
		})
	}

	if len(fails) == 0 {
		return nil
	}

	return []validationErr{
		{
			Field:  fieldChartTableOptions,
			Nested: fails,
		},
	}
}

const (
	colorTypeBackground = "background"
	colorTypeMin        = "min"
	colorTypeMax        = "max"
	colorTypeScale      = "scale"
	colorTypeText       = "text"
	colorTypeThreshold  = "threshold"
)

const (
	fieldColorHex = "hex"
)

type color struct {
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
			Type:  cc.Type,
			Hex:   cc.Hex,
			Name:  cc.Name,
			Value: ptrToFloat64(cc.Value),
		})
	}
	return iColors
}

func (c colors) strings() []string {
	clrs := []string{}

	for _, clr := range c {
		clrs = append(clrs, clr.Hex)
	}

	return clrs
}

// TODO: looks like much of these are actually getting defaults in
//  the UI. looking at sytem charts, seeign lots of failures for missing
//  color types or no colors at all.
func (c colors) hasTypes(types ...string) []validationErr {
	tMap := make(map[string]bool)
	for _, cc := range c {
		tMap[cc.Type] = true
	}

	var failures []validationErr
	for _, t := range types {
		if !tMap[t] {
			failures = append(failures, validationErr{
				Field: "colors",
				Msg:   fmt.Sprintf("type not found: %q", t),
			})
		}
	}

	return failures
}

func (c colors) valid() []validationErr {
	var fails []validationErr
	for i, cc := range c {
		cErr := validationErr{
			Field: fieldChartColors,
			Index: intPtr(i),
		}
		if cc.Hex == "" {
			cErr.Nested = append(cErr.Nested, validationErr{
				Field: fieldColorHex,
				Msg:   "a color must have a hex value provided",
			})
		}
		if len(cErr.Nested) > 0 {
			fails = append(fails, cErr)
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
		// TODO: axe this builder configs when issue https://github.com/influxdata/influxdb/issues/15708 is fixed up
		newQuery.BuilderConfig.Tags = append(newQuery.BuilderConfig.Tags, influxdb.NewBuilderTag("_measurement", "filter", ""))
		iQueries = append(iQueries, newQuery)
	}
	return iQueries
}

const (
	fieldAxisBase  = "base"
	fieldAxisLabel = "label"
	fieldAxisScale = "scale"
)

type axis struct {
	Base   string    `json:"base,omitempty" yaml:"base,omitempty"`
	Label  string    `json:"label,omitempty" yaml:"label,omitempty"`
	Name   string    `json:"name,omitempty" yaml:"name,omitempty"`
	Prefix string    `json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Scale  string    `json:"scale,omitempty" yaml:"scale,omitempty"`
	Suffix string    `json:"suffix,omitempty" yaml:"suffix,omitempty"`
	Domain []float64 `json:"domain,omitempty" yaml:"domain,omitempty"`
}

type axes []axis

func (a axes) get(name string) axis {
	for _, ax := range a {
		if name == ax.Name {
			return ax
		}
	}
	return axis{}
}

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

func (a axes) hasAxes(expectedAxes ...string) []validationErr {
	mAxes := make(map[string]bool)
	for _, ax := range a {
		mAxes[ax.Name] = true
	}

	var failures []validationErr
	for _, expected := range expectedAxes {
		if !mAxes[expected] {
			failures = append(failures, validationErr{
				Field: fieldChartAxes,
				Msg:   fmt.Sprintf("axis not found: %q", expected),
			})
		}
	}

	return failures
}

const (
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

type assocMapKey struct {
	resType influxdb.ResourceType
	name    string
}

type assocMapVal struct {
	exists bool
	v      interface{}
}

func (l assocMapVal) PkgName() string {
	t, ok := l.v.(interface{ PkgName() string })
	if ok {
		return t.PkgName()
	}
	return ""
}

type associationMapping struct {
	mappings map[assocMapKey][]assocMapVal
}

func (l *associationMapping) setMapping(v interface {
	ResourceType() influxdb.ResourceType
	Name() string
}, exists bool) {
	if l == nil {
		return
	}
	if l.mappings == nil {
		l.mappings = make(map[assocMapKey][]assocMapVal)
	}

	k := assocMapKey{
		resType: v.ResourceType(),
		name:    v.Name(),
	}
	val := assocMapVal{
		exists: exists,
		v:      v,
	}
	existing, ok := l.mappings[k]
	if !ok {
		l.mappings[k] = []assocMapVal{val}
		return
	}
	for i, ex := range existing {
		if ex.v == v {
			existing[i].exists = exists
			return
		}
	}
	l.mappings[k] = append(l.mappings[k], val)
}

const (
	fieldLabelColor = "color"
)

const labelNameMinLength = 2

type label struct {
	identity

	Color       string
	Description string
	associationMapping
}

func (l *label) summarize() SummaryLabel {
	return SummaryLabel{
		PkgName: l.PkgName(),
		Name:    l.Name(),
		Properties: struct {
			Color       string `json:"color"`
			Description string `json:"description"`
		}{
			Color:       l.Color,
			Description: l.Description,
		},
	}
}

func (l *label) mappingSummary() []SummaryLabelMapping {
	var mappings []SummaryLabelMapping
	for resource, vals := range l.mappings {
		for _, v := range vals {
			status := StateStatusNew
			if v.exists {
				status = StateStatusExists
			}
			mappings = append(mappings, SummaryLabelMapping{
				exists:          v.exists,
				Status:          status,
				ResourcePkgName: v.PkgName(),
				ResourceName:    resource.name,
				ResourceType:    resource.resType,
				LabelPkgName:    l.PkgName(),
				LabelName:       l.Name(),
			})
		}
	}

	return mappings
}

func (l *label) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(l.Name(), labelNameMinLength); !ok {
		vErrs = append(vErrs, err)
	}
	if len(vErrs) == 0 {
		return nil
	}
	return []validationErr{
		objectValidationErr(fieldSpec, vErrs...),
	}
}

func toSummaryLabels(labels ...*label) []SummaryLabel {
	iLabels := make([]SummaryLabel, 0, len(labels))
	for _, l := range labels {
		iLabels = append(iLabels, l.summarize())
	}
	return iLabels
}

type sortedLabels []*label

func (s sortedLabels) Len() int {
	return len(s)
}

func (s sortedLabels) Less(i, j int) bool {
	return s[i].PkgName() < s[j].PkgName()
}

func (s sortedLabels) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type notificationEndpointKind int

const (
	notificationKindHTTP notificationEndpointKind = iota + 1
	notificationKindPagerDuty
	notificationKindSlack
)

func (n notificationEndpointKind) String() string {
	if n > 0 && n < 4 {
		return [...]string{
			endpoint.HTTPType,
			endpoint.PagerDutyType,
			endpoint.SlackType,
		}[n-1]
	}
	return ""
}

const (
	notificationHTTPAuthTypeBasic  = "basic"
	notificationHTTPAuthTypeBearer = "bearer"
	notificationHTTPAuthTypeNone   = "none"
)

const (
	fieldNotificationEndpointHTTPMethod = "method"
	fieldNotificationEndpointPassword   = "password"
	fieldNotificationEndpointRoutingKey = "routingKey"
	fieldNotificationEndpointToken      = "token"
	fieldNotificationEndpointURL        = "url"
	fieldNotificationEndpointUsername   = "username"
)

type notificationEndpoint struct {
	identity

	kind        notificationEndpointKind
	description string
	method      string
	password    *references
	routingKey  *references
	status      string
	token       *references
	httpType    string
	url         string
	username    *references

	labels sortedLabels
}

func (n *notificationEndpoint) Labels() []*label {
	return n.labels
}

func (n *notificationEndpoint) ResourceType() influxdb.ResourceType {
	return KindNotificationEndpointSlack.ResourceType()
}

func (n *notificationEndpoint) base() endpoint.Base {
	return endpoint.Base{
		Name:        n.Name(),
		Description: n.description,
		Status:      n.influxStatus(),
	}
}

func (n *notificationEndpoint) summarize() SummaryNotificationEndpoint {
	base := n.base()
	sum := SummaryNotificationEndpoint{
		PkgName:           n.PkgName(),
		LabelAssociations: toSummaryLabels(n.labels...),
	}

	switch n.kind {
	case notificationKindHTTP:
		e := &endpoint.HTTP{
			Base:   base,
			URL:    n.url,
			Method: n.method,
		}
		switch n.httpType {
		case notificationHTTPAuthTypeBasic:
			e.AuthMethod = notificationHTTPAuthTypeBasic
			e.Password = n.password.SecretField()
			e.Username = n.username.SecretField()
		case notificationHTTPAuthTypeBearer:
			e.AuthMethod = notificationHTTPAuthTypeBearer
			e.Token = n.token.SecretField()
		case notificationHTTPAuthTypeNone:
			e.AuthMethod = notificationHTTPAuthTypeNone
		}
		sum.NotificationEndpoint = e
	case notificationKindPagerDuty:
		sum.NotificationEndpoint = &endpoint.PagerDuty{
			Base:       base,
			ClientURL:  n.url,
			RoutingKey: n.routingKey.SecretField(),
		}
	case notificationKindSlack:
		sum.NotificationEndpoint = &endpoint.Slack{
			Base:  base,
			URL:   n.url,
			Token: n.token.SecretField(),
		}
	}
	return sum
}

func (n *notificationEndpoint) influxStatus() influxdb.Status {
	status := influxdb.Active
	if n.status != "" {
		status = influxdb.Status(n.status)
	}
	return status
}

var validEndpointHTTPMethods = map[string]bool{
	"DELETE":  true,
	"GET":     true,
	"HEAD":    true,
	"OPTIONS": true,
	"PATCH":   true,
	"POST":    true,
	"PUT":     true,
}

func (n *notificationEndpoint) valid() []validationErr {
	var failures []validationErr
	if err, ok := isValidName(n.Name(), 1); !ok {
		failures = append(failures, err)
	}

	if _, err := url.Parse(n.url); err != nil || n.url == "" {
		failures = append(failures, validationErr{
			Field: fieldNotificationEndpointURL,
			Msg:   "must be valid url",
		})
	}

	status := influxdb.Status(n.status)
	if status != "" && influxdb.Inactive != status && influxdb.Active != status {
		failures = append(failures, validationErr{
			Field: fieldStatus,
			Msg:   "not a valid status; valid statues are one of [active, inactive]",
		})
	}

	switch n.kind {
	case notificationKindPagerDuty:
		if !n.routingKey.hasValue() {
			failures = append(failures, validationErr{
				Field: fieldNotificationEndpointRoutingKey,
				Msg:   "must be provide",
			})
		}
	case notificationKindHTTP:
		if !validEndpointHTTPMethods[n.method] {
			failures = append(failures, validationErr{
				Field: fieldNotificationEndpointHTTPMethod,
				Msg:   "http method must be a valid HTTP verb",
			})
		}

		switch n.httpType {
		case notificationHTTPAuthTypeBasic:
			if !n.password.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointPassword,
					Msg:   "must provide non empty string",
				})
			}
			if !n.username.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointUsername,
					Msg:   "must provide non empty string",
				})
			}
		case notificationHTTPAuthTypeBearer:
			if !n.token.hasValue() {
				failures = append(failures, validationErr{
					Field: fieldNotificationEndpointToken,
					Msg:   "must provide non empty string",
				})
			}
		case notificationHTTPAuthTypeNone:
		default:
			failures = append(failures, validationErr{
				Field: fieldType,
				Msg: fmt.Sprintf(
					"invalid type provided %q; valid type is 1 in [%s, %s, %s]",
					n.httpType,
					notificationHTTPAuthTypeBasic,
					notificationHTTPAuthTypeBearer,
					notificationHTTPAuthTypeNone,
				),
			})
		}
	}

	if len(failures) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, failures...),
		}
	}

	return nil
}

const (
	fieldNotificationRuleChannel         = "channel"
	fieldNotificationRuleCurrentLevel    = "currentLevel"
	fieldNotificationRuleEndpointName    = "endpointName"
	fieldNotificationRuleMessageTemplate = "messageTemplate"
	fieldNotificationRulePreviousLevel   = "previousLevel"
	fieldNotificationRuleStatusRules     = "statusRules"
	fieldNotificationRuleTagRules        = "tagRules"
)

type notificationRule struct {
	identity

	channel     string
	description string
	every       time.Duration
	msgTemplate string
	offset      time.Duration
	status      string
	statusRules []struct{ curLvl, prevLvl string }
	tagRules    []struct{ k, v, op string }

	associatedEndpoint *notificationEndpoint
	endpointName       *references

	labels sortedLabels
}

func (r *notificationRule) Labels() []*label {
	return r.labels
}

func (r *notificationRule) ResourceType() influxdb.ResourceType {
	return KindNotificationRule.ResourceType()
}

func (r *notificationRule) Status() influxdb.Status {
	if r.status == "" {
		return influxdb.Active
	}
	return influxdb.Status(r.status)
}

func (r *notificationRule) endpointPkgName() string {
	if r.associatedEndpoint != nil {
		return r.associatedEndpoint.PkgName()
	}
	return ""
}

func (r *notificationRule) summarize() SummaryNotificationRule {
	var endpointPkgName, endpointType string
	if r.associatedEndpoint != nil {
		endpointPkgName = r.associatedEndpoint.PkgName()
		endpointType = r.associatedEndpoint.kind.String()
	}

	return SummaryNotificationRule{
		PkgName:           r.PkgName(),
		Name:              r.Name(),
		EndpointPkgName:   endpointPkgName,
		EndpointType:      endpointType,
		Description:       r.description,
		Every:             r.every.String(),
		LabelAssociations: toSummaryLabels(r.labels...),
		Offset:            r.offset.String(),
		MessageTemplate:   r.msgTemplate,
		Status:            r.Status(),
		StatusRules:       toSummaryStatusRules(r.statusRules),
		TagRules:          toSummaryTagRules(r.tagRules),
	}
}

func (r *notificationRule) toInfluxRule() influxdb.NotificationRule {
	base := rule.Base{
		Name:        r.Name(),
		Description: r.description,
		Every:       toNotificationDuration(r.every),
		Offset:      toNotificationDuration(r.offset),
	}
	for _, sr := range r.statusRules {
		var prevLvl *notification.CheckLevel
		if lvl := notification.ParseCheckLevel(sr.prevLvl); lvl != notification.Unknown {
			prevLvl = &lvl
		}
		base.StatusRules = append(base.StatusRules, notification.StatusRule{
			CurrentLevel:  notification.ParseCheckLevel(sr.curLvl),
			PreviousLevel: prevLvl,
		})
	}
	for _, tr := range r.tagRules {
		op, _ := influxdb.ToOperator(tr.op)
		base.TagRules = append(base.TagRules, notification.TagRule{
			Tag: influxdb.Tag{
				Key:   tr.k,
				Value: tr.v,
			},
			Operator: op,
		})
	}

	switch r.associatedEndpoint.kind {
	case notificationKindHTTP:
		return &rule.HTTP{Base: base}
	case notificationKindPagerDuty:
		return &rule.PagerDuty{
			Base:            base,
			MessageTemplate: r.msgTemplate,
		}
	case notificationKindSlack:
		return &rule.Slack{
			Base:            base,
			Channel:         r.channel,
			MessageTemplate: r.msgTemplate,
		}
	}
	return nil
}

func (r *notificationRule) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(r.Name(), 1); !ok {
		vErrs = append(vErrs, err)
	}
	if !r.endpointName.hasValue() {
		vErrs = append(vErrs, validationErr{
			Field: fieldNotificationRuleEndpointName,
			Msg:   "must be provided",
		})
	} else if r.associatedEndpoint == nil {
		vErrs = append(vErrs, validationErr{
			Field: fieldNotificationRuleEndpointName,
			Msg:   fmt.Sprintf("notification endpoint %q does not exist in pkg", r.endpointName.String()),
		})
	}

	if r.every == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldEvery,
			Msg:   "must be provided",
		})
	}
	if status := r.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   fmt.Sprintf("must be 1 in [active, inactive]; got=%q", r.status),
		})
	}

	if len(r.statusRules) == 0 {
		vErrs = append(vErrs, validationErr{
			Field: fieldNotificationRuleStatusRules,
			Msg:   "must provide at least 1",
		})
	}

	var sRuleErrs []validationErr
	for i, sRule := range r.statusRules {
		if notification.ParseCheckLevel(sRule.curLvl) == notification.Unknown {
			sRuleErrs = append(sRuleErrs, validationErr{
				Field: fieldNotificationRuleCurrentLevel,
				Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", sRule.curLvl),
				Index: intPtr(i),
			})
		}
		if sRule.prevLvl != "" && notification.ParseCheckLevel(sRule.prevLvl) == notification.Unknown {
			sRuleErrs = append(sRuleErrs, validationErr{
				Field: fieldNotificationRulePreviousLevel,
				Msg:   fmt.Sprintf("must be 1 in [CRIT, WARN, INFO, OK]; got=%q", sRule.prevLvl),
				Index: intPtr(i),
			})
		}
	}
	if len(sRuleErrs) > 0 {
		vErrs = append(vErrs, validationErr{
			Field:  fieldNotificationRuleStatusRules,
			Nested: sRuleErrs,
		})
	}

	var tagErrs []validationErr
	for i, tRule := range r.tagRules {
		if _, ok := influxdb.ToOperator(tRule.op); !ok {
			tagErrs = append(tagErrs, validationErr{
				Field: fieldOperator,
				Msg:   fmt.Sprintf("must be 1 in [equal]; got=%q", tRule.op),
				Index: intPtr(i),
			})
		}
	}
	if len(tagErrs) > 0 {
		vErrs = append(vErrs, validationErr{
			Field:  fieldNotificationRuleTagRules,
			Nested: tagErrs,
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

func toSummaryStatusRules(statusRules []struct{ curLvl, prevLvl string }) []SummaryStatusRule {
	out := make([]SummaryStatusRule, 0, len(statusRules))
	for _, sRule := range statusRules {
		out = append(out, SummaryStatusRule{
			CurrentLevel:  sRule.curLvl,
			PreviousLevel: sRule.prevLvl,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := out[i], out[j]
		if si.CurrentLevel == sj.CurrentLevel {
			return si.PreviousLevel < sj.PreviousLevel
		}
		return si.CurrentLevel < sj.CurrentLevel
	})
	return out
}

func toSummaryTagRules(tagRules []struct{ k, v, op string }) []SummaryTagRule {
	out := make([]SummaryTagRule, 0, len(tagRules))
	for _, tRule := range tagRules {
		out = append(out, SummaryTagRule{
			Key:      tRule.k,
			Value:    tRule.v,
			Operator: tRule.op,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		ti, tj := out[i], out[j]
		if ti.Key == tj.Key && ti.Value == tj.Value {
			return ti.Operator < tj.Operator
		}
		if ti.Key == tj.Key {
			return ti.Value < tj.Value
		}
		return ti.Key < tj.Key
	})
	return out
}

const (
	fieldTaskCron = "cron"
)

type task struct {
	identity

	cron        string
	description string
	every       time.Duration
	offset      time.Duration
	query       string
	status      string

	labels sortedLabels
}

func (t *task) Labels() []*label {
	return t.labels
}

func (t *task) ResourceType() influxdb.ResourceType {
	return KindTask.ResourceType()
}

func (t *task) Status() influxdb.Status {
	if t.status == "" {
		return influxdb.Active
	}
	return influxdb.Status(t.status)
}

func (t *task) flux() string {
	translator := taskFluxTranslation{
		name:     t.Name(),
		cron:     t.cron,
		every:    t.every,
		offset:   t.offset,
		rawQuery: t.query,
	}
	return translator.flux()
}

func (t *task) summarize() SummaryTask {
	return SummaryTask{
		PkgName:     t.PkgName(),
		Name:        t.Name(),
		Cron:        t.cron,
		Description: t.description,
		Every:       durToStr(t.every),
		Offset:      durToStr(t.offset),
		Query:       t.query,
		Status:      t.Status(),

		LabelAssociations: toSummaryLabels(t.labels...),
	}
}

func (t *task) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(t.Name(), 1); !ok {
		vErrs = append(vErrs, err)
	}
	if t.cron == "" && t.every == 0 {
		vErrs = append(vErrs,
			validationErr{
				Field: fieldEvery,
				Msg:   "must provide if cron field is not provided",
			},
			validationErr{
				Field: fieldTaskCron,
				Msg:   "must provide if every field is not provided",
			},
		)
	}

	if t.query == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldQuery,
			Msg:   "must provide a non zero value",
		})
	}

	if status := t.Status(); status != influxdb.Active && status != influxdb.Inactive {
		vErrs = append(vErrs, validationErr{
			Field: fieldStatus,
			Msg:   "must be 1 of [active, inactive]",
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

var fluxRegex = regexp.MustCompile(`import\s+\".*\"`)

type taskFluxTranslation struct {
	name   string
	cron   string
	every  time.Duration
	offset time.Duration

	rawQuery string
}

func (tft taskFluxTranslation) flux() string {
	var sb strings.Builder
	writeLine := func(s string) {
		sb.WriteString(s + "\n")
	}

	imports, queryBody := tft.separateQueryImports()
	if imports != "" {
		writeLine(imports + "\n")
	}

	writeLine(tft.generateTaskOption())
	sb.WriteString(queryBody)

	return sb.String()
}

func (tft taskFluxTranslation) separateQueryImports() (imports string, querySansImports string) {
	if indices := fluxRegex.FindAllIndex([]byte(tft.rawQuery), -1); len(indices) > 0 {
		lastImportIdx := indices[len(indices)-1][1]
		return tft.rawQuery[:lastImportIdx], tft.rawQuery[lastImportIdx:]
	}

	return "", tft.rawQuery
}

func (tft taskFluxTranslation) generateTaskOption() string {
	taskOpts := []string{fmt.Sprintf("name: %q", tft.name)}
	if tft.cron != "" {
		taskOpts = append(taskOpts, fmt.Sprintf("cron: %q", tft.cron))
	}
	if tft.every > 0 {
		taskOpts = append(taskOpts, fmt.Sprintf("every: %s", tft.every))
	}
	if tft.offset > 0 {
		taskOpts = append(taskOpts, fmt.Sprintf("offset: %s", tft.offset))
	}

	// this is required by the API, super nasty. Will be super challenging for
	// anyone outside org to figure out how to do this within an hour of looking
	// at the API :sadpanda:. Would be ideal to let the API translate the arguments
	// into this required form instead of forcing that complexity on the caller.
	return fmt.Sprintf("option task = { %s }", strings.Join(taskOpts, ", "))
}

const (
	fieldTelegrafConfig = "config"
)

type telegraf struct {
	identity

	config influxdb.TelegrafConfig

	labels sortedLabels
}

func (t *telegraf) Labels() []*label {
	return t.labels
}

func (t *telegraf) ResourceType() influxdb.ResourceType {
	return KindTelegraf.ResourceType()
}

func (t *telegraf) summarize() SummaryTelegraf {
	cfg := t.config
	cfg.Name = t.Name()
	return SummaryTelegraf{
		PkgName:           t.PkgName(),
		TelegrafConfig:    cfg,
		LabelAssociations: toSummaryLabels(t.labels...),
	}
}

func (t *telegraf) valid() []validationErr {
	var vErrs []validationErr
	if err, ok := isValidName(t.Name(), 1); !ok {
		vErrs = append(vErrs, err)
	}
	if t.config.Config == "" {
		vErrs = append(vErrs, validationErr{
			Field: fieldTelegrafConfig,
			Msg:   "no config provided",
		})
	}

	if len(vErrs) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, vErrs...),
		}
	}

	return nil
}

const (
	fieldArgTypeConstant = "constant"
	fieldArgTypeMap      = "map"
	fieldArgTypeQuery    = "query"
)

type variable struct {
	identity

	Description string
	Type        string
	Query       string
	Language    string
	ConstValues []string
	MapValues   map[string]string

	labels sortedLabels
}

func (v *variable) Labels() []*label {
	return v.labels
}

func (v *variable) ResourceType() influxdb.ResourceType {
	return KindVariable.ResourceType()
}

func (v *variable) summarize() SummaryVariable {
	return SummaryVariable{
		PkgName:           v.PkgName(),
		Name:              v.Name(),
		Description:       v.Description,
		Arguments:         v.influxVarArgs(),
		LabelAssociations: toSummaryLabels(v.labels...),
	}
}

func (v *variable) influxVarArgs() *influxdb.VariableArguments {
	// this zero value check is for situations where we want to marshal/unmarshal
	// a variable and not have the invalid args blow up during unmarshaling. When
	// that validation is decoupled from the unmarshaling, we can clean this up.
	if v.Type == "" {
		return nil
	}

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

func (v *variable) valid() []validationErr {
	var failures []validationErr
	if err, ok := isValidName(v.Name(), 1); !ok {
		failures = append(failures, err)
	}

	switch v.Type {
	case "map":
		if len(v.MapValues) == 0 {
			failures = append(failures, validationErr{
				Field: fieldValues,
				Msg:   "map variable must have at least 1 key/val pair",
			})
		}
	case "constant":
		if len(v.ConstValues) == 0 {
			failures = append(failures, validationErr{
				Field: fieldValues,
				Msg:   "constant variable must have a least 1 value provided",
			})
		}
	case "query":
		if v.Query == "" {
			failures = append(failures, validationErr{
				Field: fieldQuery,
				Msg:   "query variable must provide a query string",
			})
		}
		if v.Language != "influxql" && v.Language != "flux" {
			failures = append(failures, validationErr{
				Field: fieldLanguage,
				Msg:   fmt.Sprintf(`query variable language must be either "influxql" or "flux"; got %q`, v.Language),
			})
		}
	}
	if len(failures) > 0 {
		return []validationErr{
			objectValidationErr(fieldSpec, failures...),
		}
	}

	return nil
}

const (
	fieldReferencesEnv    = "envRef"
	fieldReferencesSecret = "secretRef"
)

type references struct {
	val    interface{}
	EnvRef string
	Secret string
}

func (r *references) hasValue() bool {
	return r.EnvRef != "" || r.Secret != "" || r.val != nil
}

func (r *references) String() string {
	if r == nil {
		return ""
	}
	if v := r.StringVal(); v != "" {
		return v
	}
	if r.EnvRef != "" {
		return "env-" + r.EnvRef
	}
	return ""
}

func (r *references) StringVal() string {
	if r.val != nil {
		s, _ := r.val.(string)
		return s
	}
	return ""
}

func (r *references) SecretField() influxdb.SecretField {
	if secret := r.Secret; secret != "" {
		return influxdb.SecretField{Key: secret}
	}
	if str := r.StringVal(); str != "" {
		return influxdb.SecretField{Value: &str}
	}
	return influxdb.SecretField{}
}

func isValidName(name string, minLength int) (validationErr, bool) {
	if len(name) >= minLength {
		return validationErr{}, true
	}
	return validationErr{
		Field: fieldName,
		Msg:   fmt.Sprintf("must be a string of at least %d chars in length", minLength),
	}, false
}

func toNotificationDuration(dur time.Duration) *notification.Duration {
	d, _ := notification.FromTimeDuration(dur)
	return &d
}

func durToStr(dur time.Duration) string {
	if dur == 0 {
		return ""
	}
	return dur.String()
}

func flt64Ptr(f float64) *float64 {
	if f != 0 {
		return &f
	}
	return nil
}

func intPtr(i int) *int {
	return &i
}
