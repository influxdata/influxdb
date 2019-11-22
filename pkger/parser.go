package pkger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"gopkg.in/yaml.v3"
)

// ReaderFn is used for functional inputs to abstract the individual
// entrypoints for the reader itself.
type ReaderFn func() (io.Reader, error)

// Encoding describes the encoding for the raw package data. The
// encoding determines how the raw data is parsed.
type Encoding int

// encoding types
const (
	EncodingUnknown Encoding = iota
	EncodingYAML
	EncodingJSON
)

// String provides the string representation of the encoding.
func (e Encoding) String() string {
	switch e {
	case EncodingJSON:
		return "json"
	case EncodingYAML:
		return "yaml"
	default:
		return "unknown"
	}
}

// ErrInvalidEncoding indicates the encoding is invalid type for the parser.
var ErrInvalidEncoding = errors.New("invalid encoding provided")

// Parse parses a pkg defined by the encoding and readerFns. As of writing this
// we can parse both a YAML and JSON format of the Pkg model.
func Parse(encoding Encoding, readerFn ReaderFn, opts ...ValidateOptFn) (*Pkg, error) {
	r, err := readerFn()
	if err != nil {
		return nil, err
	}

	switch encoding {
	case EncodingYAML:
		return parseYAML(r, opts...)
	case EncodingJSON:
		return parseJSON(r, opts...)
	default:
		return nil, ErrInvalidEncoding
	}
}

// FromFile reads a file from disk and provides a reader from it.
func FromFile(filePath string) ReaderFn {
	return func() (io.Reader, error) {
		// not using os.Open to avoid having to deal with closing the file in here
		b, err := ioutil.ReadFile(filePath)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(b), nil
	}
}

// FromReader simply passes the reader along. Useful when consuming
// this from an HTTP request body. There are a number of other useful
// places for this functional input.
func FromReader(r io.Reader) ReaderFn {
	return func() (io.Reader, error) {
		return r, nil
	}
}

// FromString parses a pkg from a raw string value. This is very useful
// in tests.
func FromString(s string) ReaderFn {
	return func() (io.Reader, error) {
		return strings.NewReader(s), nil
	}
}

func parseYAML(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
	return parse(yaml.NewDecoder(r), opts...)
}

func parseJSON(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
	return parse(json.NewDecoder(r), opts...)
}

type decoder interface {
	Decode(interface{}) error
}

func parse(dec decoder, opts ...ValidateOptFn) (*Pkg, error) {
	var pkg Pkg
	if err := dec.Decode(&pkg); err != nil {
		return nil, err
	}

	if err := pkg.Validate(opts...); err != nil {
		return nil, err
	}

	return &pkg, nil
}

// Pkg is the model for a package. The resources are more generic that one might
// expect at first glance. This was done on purpose. The way json/yaml/toml or
// w/e scripting you want to use, can have very different ways of parsing. The
// different parsers are limited for the parsers that do not come from the std
// lib (looking at you yaml/v2). This allows us to parse it and leave the matching
// to another power, the graphing of the package is handled within itself.
type Pkg struct {
	APIVersion string   `yaml:"apiVersion" json:"apiVersion"`
	Kind       Kind     `yaml:"kind" json:"kind"`
	Metadata   Metadata `yaml:"meta" json:"meta"`
	Spec       struct {
		Resources []Resource `yaml:"resources" json:"resources"`
	} `yaml:"spec" json:"spec"`

	mLabels     map[string]*label
	mBuckets    map[string]*bucket
	mDashboards map[string]*dashboard
	mVariables  map[string]*variable

	isVerified bool // dry run has verified pkg resources with existing resources
	isParsed   bool // indicates the pkg has been parsed and all resources graphed accordingly
}

// Summary returns a package Summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Pkg) Summary() Summary {
	var sum Summary

	for _, b := range p.buckets() {
		sum.Buckets = append(sum.Buckets, b.summarize())
	}

	for _, d := range p.dashboards() {
		sum.Dashboards = append(sum.Dashboards, d.summarize())
	}

	for _, l := range p.labels() {
		sum.Labels = append(sum.Labels, l.summarize())
	}

	for _, m := range p.labelMappings() {
		sum.LabelMappings = append(sum.LabelMappings, SummaryLabelMapping{
			ResourceName: m.ResourceName,
			LabelName:    m.LabelName,
			LabelMapping: m.LabelMapping,
		})
	}

	for _, v := range p.variables() {
		sum.Variables = append(sum.Variables, v.summarize())
	}

	return sum
}

type (
	validateOpt struct {
		minResources bool
	}

	// ValidateOptFn provides a means to disable desired validation checks.
	ValidateOptFn func(*validateOpt)
)

// ValidWithoutResources ignores the validation check for minimum number
// of resources. This is useful for the service Create to ignore this and
// allow the creation of a pkg without resources.
func ValidWithoutResources() ValidateOptFn {
	return func(opt *validateOpt) {
		opt.minResources = false
	}
}

// Validate will graph all resources and validate every thing is in a useful form.
func (p *Pkg) Validate(opts ...ValidateOptFn) error {
	opt := &validateOpt{minResources: true}
	for _, o := range opts {
		o(opt)
	}
	setupFns := []func() error{
		p.validMetadata,
	}
	if opt.minResources {
		setupFns = append(setupFns, p.validResources)
	}
	setupFns = append(setupFns, p.graphResources)

	var pErr parseErr
	for _, fn := range setupFns {
		if err := fn(); err != nil {
			if IsParseErr(err) {
				pErr.append(err.(*parseErr).Resources...)
				continue
			}
			return err
		}
	}

	if len(pErr.Resources) > 0 {
		return &pErr
	}

	p.isParsed = true
	return nil
}

func (p *Pkg) buckets() []*bucket {
	buckets := make([]*bucket, 0, len(p.mBuckets))
	for _, b := range p.mBuckets {
		buckets = append(buckets, b)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets
}

func (p *Pkg) labels() []*label {
	labels := make([]*label, 0, len(p.mLabels))
	for _, b := range p.mLabels {
		labels = append(labels, b)
	}

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	return labels
}

func (p *Pkg) dashboards() []*dashboard {
	dashes := make([]*dashboard, 0, len(p.mDashboards))
	for _, d := range p.mDashboards {
		dashes = append(dashes, d)
	}

	sort.Slice(dashes, func(i, j int) bool {
		return dashes[i].Name < dashes[j].Name
	})

	return dashes
}

func (p *Pkg) variables() []*variable {
	vars := make([]*variable, 0, len(p.mVariables))
	for _, v := range p.mVariables {
		vars = append(vars, v)
	}

	sort.Slice(vars, func(i, j int) bool {
		return vars[i].Name < vars[j].Name
	})

	return vars
}

// labelMappings returns the mappings that will be created for
// valid pairs of labels and resources of which all have IDs.
// If a resource does not exist yet, a label mapping will not
// be returned for it.
func (p *Pkg) labelMappings() []SummaryLabelMapping {
	var mappings []SummaryLabelMapping
	for _, l := range p.mLabels {
		mappings = append(mappings, l.mappingSummary()...)
	}

	// sort by res type ASC, then res name ASC, then label name ASC
	sort.Slice(mappings, func(i, j int) bool {
		n, m := mappings[i], mappings[j]
		if n.ResourceType < m.ResourceType {
			return true
		}
		if n.ResourceType > m.ResourceType {
			return false
		}
		if n.ResourceName < m.ResourceName {
			return true
		}
		if n.ResourceName > m.ResourceName {
			return false
		}
		return n.LabelName < m.LabelName
	})

	return mappings
}

func (p *Pkg) validMetadata() error {
	var failures []validationErr
	if p.APIVersion != APIVersion {
		failures = append(failures, validationErr{
			Field: "apiVersion",
			Msg:   "must be version " + APIVersion,
		})
	}

	if !p.Kind.is(KindPackage) {
		failures = append(failures, validationErr{
			Field: "kind",
			Msg:   `must be of kind "Package"`,
		})
	}

	var metaFails []validationErr
	if p.Metadata.Version == "" {
		metaFails = append(metaFails, validationErr{
			Field: "pkgVersion",
			Msg:   "version is required",
		})
	}

	if p.Metadata.Name == "" {
		metaFails = append(metaFails, validationErr{
			Field: "pkgName",
			Msg:   "must be at least 1 char",
		})
	}

	if len(metaFails) > 0 {
		failures = append(failures, validationErr{
			Field:  "meta",
			Nested: metaFails,
		})
	}

	if len(failures) == 0 {
		return nil
	}

	var err parseErr
	err.append(resourceErr{
		Kind:     KindPackage.String(),
		RootErrs: failures,
	})
	return &err
}

func (p *Pkg) validResources() error {
	if len(p.Spec.Resources) > 0 {
		return nil
	}

	res := resourceErr{
		Kind: "Package",
		RootErrs: []validationErr{{
			Field: "resources",
			Msg:   "at least 1 resource must be provided",
		}},
	}
	var err parseErr
	err.append(res)
	return &err
}

func (p *Pkg) graphResources() error {
	graphFns := []func() error{
		// labels are first to validate associations with other resources
		p.graphLabels,
		p.graphVariables,
		p.graphBuckets,
		p.graphDashboards,
	}

	var pErr parseErr
	for _, fn := range graphFns {
		if err := fn(); err != nil {
			if IsParseErr(err) {
				pErr.append(err.(*parseErr).Resources...)
				continue
			}
			return err
		}
	}

	if len(pErr.Resources) > 0 {
		sort.Slice(pErr.Resources, func(i, j int) bool {
			ir, jr := pErr.Resources[i], pErr.Resources[j]
			return *ir.Idx < *jr.Idx
		})
		return &pErr
	}

	return nil
}

func (p *Pkg) graphBuckets() error {
	p.mBuckets = make(map[string]*bucket)
	return p.eachResource(KindBucket, func(r Resource) []validationErr {
		if r.Name() == "" {
			return []validationErr{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		if _, ok := p.mBuckets[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		bkt := &bucket{
			Name:        r.Name(),
			Description: r.stringShort(fieldDescription),
		}
		if rules, ok := r[fieldBucketRetentionRules].(retentionRules); ok {
			bkt.RetentionRules = rules
		} else {
			for _, r := range r.slcResource(fieldBucketRetentionRules) {
				bkt.RetentionRules = append(bkt.RetentionRules, retentionRule{
					Type:    r.stringShort(fieldType),
					Seconds: r.intShort(fieldRetentionRulesEverySeconds),
				})
			}
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			bkt.labels = append(bkt.labels, l)
			p.mLabels[l.Name].setBucketMapping(bkt, false)
			return nil
		})
		sort.Slice(bkt.labels, func(i, j int) bool {
			return bkt.labels[i].Name < bkt.labels[j].Name
		})

		p.mBuckets[r.Name()] = bkt

		return append(failures, bkt.valid()...)
	})
}

func (p *Pkg) graphLabels() error {
	p.mLabels = make(map[string]*label)
	return p.eachResource(KindLabel, func(r Resource) []validationErr {
		if r.Name() == "" {
			return []validationErr{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		if _, ok := p.mLabels[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}
		p.mLabels[r.Name()] = &label{
			Name:        r.Name(),
			Color:       r.stringShort(fieldLabelColor),
			Description: r.stringShort(fieldDescription),
		}

		return nil
	})
}

func (p *Pkg) graphDashboards() error {
	p.mDashboards = make(map[string]*dashboard)
	return p.eachResource(KindDashboard, func(r Resource) []validationErr {
		if r.Name() == "" {
			return []validationErr{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		dash := &dashboard{
			Name:        r.Name(),
			Description: r.stringShort(fieldDescription),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			dash.labels = append(dash.labels, l)
			p.mLabels[l.Name].setDashboardMapping(dash)
			return nil
		})
		sort.Slice(dash.labels, func(i, j int) bool {
			return dash.labels[i].Name < dash.labels[j].Name
		})

		for i, cr := range r.slcResource(fieldDashCharts) {
			ch, fails := parseChart(cr)
			if fails != nil {
				failures = append(failures, validationErr{
					Field:  "charts",
					Index:  intPtr(i),
					Nested: fails,
				})
				continue
			}
			dash.Charts = append(dash.Charts, ch)
		}

		p.mDashboards[r.Name()] = dash

		return failures
	})
}

func (p *Pkg) graphVariables() error {
	p.mVariables = make(map[string]*variable)
	return p.eachResource(KindVariable, func(r Resource) []validationErr {
		if r.Name() == "" {
			return []validationErr{{
				Field: "name",
				Msg:   "must be provided",
			}}
		}

		if _, ok := p.mVariables[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		newVar := &variable{
			Name:        r.Name(),
			Description: r.stringShort(fieldDescription),
			Type:        strings.ToLower(r.stringShort(fieldType)),
			Query:       strings.TrimSpace(r.stringShort(fieldQuery)),
			Language:    strings.ToLower(strings.TrimSpace(r.stringShort(fieldLanguage))),
			ConstValues: r.slcStr(fieldValues),
			MapValues:   r.mapStrStr(fieldValues),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			newVar.labels = append(newVar.labels, l)
			p.mLabels[l.Name].setVariableMapping(newVar, false)
			return nil
		})
		sort.Slice(newVar.labels, func(i, j int) bool {
			return newVar.labels[i].Name < newVar.labels[j].Name
		})

		p.mVariables[r.Name()] = newVar

		return append(failures, newVar.valid()...)
	})
}

func (p *Pkg) eachResource(resourceKind Kind, fn func(r Resource) []validationErr) error {
	var pErr parseErr
	for i, r := range p.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			pErr.append(resourceErr{
				Kind: k.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					{
						Field: "kind",
						Msg:   err.Error(),
					},
				},
			})
			continue
		}
		if !k.is(resourceKind) {
			continue
		}

		if failures := fn(r); failures != nil {
			err := resourceErr{
				Kind: resourceKind.String(),
				Idx:  intPtr(i),
			}
			for _, f := range failures {
				vErr := validationErr{
					Field:  f.Field,
					Msg:    f.Msg,
					Index:  f.Index,
					Nested: f.Nested,
				}
				if vErr.Field == "associations" {
					err.AssociationErrs = append(err.AssociationErrs, vErr)
					continue
				}
				err.ValidationErrs = append(err.ValidationErrs, vErr)
			}
			pErr.append(err)
		}
	}

	if len(pErr.Resources) > 0 {
		return &pErr
	}
	return nil
}

func (p *Pkg) parseNestedLabels(r Resource, fn func(lb *label) error) []validationErr {
	nestedLabels := make(map[string]*label)

	var failures []validationErr
	for i, nr := range r.slcResource(fieldAssociations) {
		fail := p.parseNestedLabel(nr, func(l *label) error {
			if _, ok := nestedLabels[l.Name]; ok {
				return fmt.Errorf("duplicate nested label: %q", l.Name)
			}
			nestedLabels[l.Name] = l

			return fn(l)
		})
		if fail != nil {
			fail.Index = intPtr(i)
			failures = append(failures, *fail)
		}
	}

	return failures
}

func (p *Pkg) parseNestedLabel(nr Resource, fn func(lb *label) error) *validationErr {
	k, err := nr.kind()
	if err != nil {
		return &validationErr{
			Field: "associations",
			Nested: []validationErr{
				{
					Field: "kind",
					Msg:   err.Error(),
				},
			},
		}
	}
	if !k.is(KindLabel) {
		return nil
	}

	lb, found := p.mLabels[nr.Name()]
	if !found {
		return &validationErr{
			Field: "associations",
			Msg:   fmt.Sprintf("label %q does not exist in pkg", nr.Name()),
		}
	}

	if err := fn(lb); err != nil {
		return &validationErr{
			Field: "associations",
			Msg:   err.Error(),
		}
	}
	return nil
}

func parseChart(r Resource) (chart, []validationErr) {
	ck, err := r.chartKind()
	if err != nil {
		return chart{}, []validationErr{{
			Field: "kind",
			Msg:   err.Error(),
		}}
	}

	c := chart{
		Kind:        ck,
		Name:        r.Name(),
		Prefix:      r.stringShort(fieldPrefix),
		Suffix:      r.stringShort(fieldSuffix),
		Note:        r.stringShort(fieldChartNote),
		NoteOnEmpty: r.boolShort(fieldChartNoteOnEmpty),
		Shade:       r.boolShort(fieldChartShade),
		XCol:        r.stringShort(fieldChartXCol),
		YCol:        r.stringShort(fieldChartYCol),
		XPos:        r.intShort(fieldChartXPos),
		YPos:        r.intShort(fieldChartYPos),
		Height:      r.intShort(fieldChartHeight),
		Width:       r.intShort(fieldChartWidth),
		Geom:        r.stringShort(fieldChartGeom),
		BinSize:     r.intShort(fieldChartBinSize),
		BinCount:    r.intShort(fieldChartBinCount),
		Position:    r.stringShort(fieldChartPosition),
	}

	if presLeg, ok := r[fieldChartLegend].(legend); ok {
		c.Legend = presLeg
	} else {
		if leg, ok := ifaceToResource(r[fieldChartLegend]); ok {
			c.Legend.Type = leg.stringShort(fieldType)
			c.Legend.Orientation = leg.stringShort(fieldLegendOrientation)
		}
	}

	if dp, ok := r.int(fieldChartDecimalPlaces); ok {
		c.EnforceDecimals = true
		c.DecimalPlaces = dp
	}

	var failures []validationErr
	if presentQueries, ok := r[fieldChartQueries].(queries); ok {
		c.Queries = presentQueries
	} else {
		for _, rq := range r.slcResource(fieldChartQueries) {
			c.Queries = append(c.Queries, query{
				Query: strings.TrimSpace(rq.stringShort(fieldQuery)),
			})
		}
	}

	if presentColors, ok := r[fieldChartColors].(colors); ok {
		c.Colors = presentColors
	} else {
		for _, rc := range r.slcResource(fieldChartColors) {
			c.Colors = append(c.Colors, &color{
				// TODO: think we can just axe the stub here
				id:    influxdb.ID(int(time.Now().UnixNano())).String(),
				Name:  rc.Name(),
				Type:  rc.stringShort(fieldType),
				Hex:   rc.stringShort(fieldColorHex),
				Value: flt64Ptr(rc.float64Short(fieldValue)),
			})
		}
	}

	if presAxes, ok := r[fieldChartAxes].(axes); ok {
		c.Axes = presAxes
	} else {
		for _, ra := range r.slcResource(fieldChartAxes) {
			domain := []float64{}

			if _, ok := ra[fieldChartDomain]; ok {
				for _, str := range ra.slcStr(fieldChartDomain) {
					val, err := strconv.ParseFloat(str, 64)
					if err != nil {
						failures = append(failures, validationErr{
							Field: "axes",
							Msg:   err.Error(),
						})
					}
					domain = append(domain, val)
				}
			}

			c.Axes = append(c.Axes, axis{
				Base:   ra.stringShort(fieldAxisBase),
				Label:  ra.stringShort(fieldAxisLabel),
				Name:   ra.Name(),
				Prefix: ra.stringShort(fieldPrefix),
				Scale:  ra.stringShort(fieldAxisScale),
				Suffix: ra.stringShort(fieldSuffix),
				Domain: domain,
			})
		}
	}

	if failures = append(failures, c.validProperties()...); len(failures) > 0 {
		return chart{}, failures
	}

	return c, nil
}

// Resource is a pkger Resource kind. It can be one of any of
// available kinds that are supported.
type Resource map[string]interface{}

// Name returns the name of the resource.
func (r Resource) Name() string {
	return strings.TrimSpace(r.stringShort(fieldName))
}

func (r Resource) kind() (Kind, error) {
	if k, ok := r[fieldKind].(Kind); ok {
		return k, k.OK()
	}

	resKind, ok := r.string(fieldKind)
	if !ok {
		return KindUnknown, errors.New("no kind provided")
	}

	k := NewKind(resKind)
	return k, k.OK()
}

func (r Resource) chartKind() (chartKind, error) {
	ck, _ := r.kind()
	chartKind := chartKind(ck)
	if !chartKind.ok() {
		return chartKindUnknown, errors.New("invalid chart kind provided: " + string(chartKind))
	}
	return chartKind, nil
}

func (r Resource) bool(key string) (bool, bool) {
	b, ok := r[key].(bool)
	return b, ok
}

func (r Resource) boolShort(key string) bool {
	b, _ := r.bool(key)
	return b
}

func (r Resource) float64(key string) (float64, bool) {
	f, ok := r[key].(float64)
	if ok {
		return f, true
	}

	i, ok := r[key].(int)
	if ok {
		return float64(i), true
	}
	return 0, false
}

func (r Resource) float64Short(key string) float64 {
	f, _ := r.float64(key)
	return f
}

func (r Resource) int(key string) (int, bool) {
	i, ok := r[key].(int)
	if ok {
		return i, true
	}

	f, ok := r[key].(float64)
	if ok {
		return int(f), true
	}
	return 0, false
}

func (r Resource) intShort(key string) int {
	i, _ := r.int(key)
	return i
}

func (r Resource) string(key string) (string, bool) {
	return ifaceToStr(r[key])
}

func (r Resource) stringShort(key string) string {
	s, _ := r.string(key)
	return s
}

func (r Resource) slcResource(key string) []Resource {
	v, ok := r[key]
	if !ok {
		return nil
	}

	if resources, ok := v.([]Resource); ok {
		return resources
	}

	iFaceSlc, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var newResources []Resource
	for _, iFace := range iFaceSlc {
		r, ok := ifaceToResource(iFace)
		if !ok {
			continue
		}
		newResources = append(newResources, r)
	}

	return newResources
}

func (r Resource) slcStr(key string) []string {
	v, ok := r[key]
	if !ok {
		return nil
	}

	if strSlc, ok := v.([]string); ok {
		return strSlc
	}

	iFaceSlc, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var out []string
	for _, iface := range iFaceSlc {
		s, ok := ifaceToStr(iface)
		if !ok {
			continue
		}
		out = append(out, s)
	}

	return out
}

func (r Resource) mapStrStr(key string) map[string]string {
	v, ok := r[key]
	if !ok {
		return nil
	}

	if m, ok := v.(map[string]string); ok {
		return m
	}

	res, ok := ifaceToResource(v)
	if !ok {
		return nil
	}

	m := make(map[string]string)
	for k, v := range res {
		s, ok := ifaceToStr(v)
		if !ok {
			continue
		}
		m[k] = s
	}
	return m
}

func ifaceToResource(i interface{}) (Resource, bool) {
	if i == nil {
		return nil, false
	}

	if res, ok := i.(Resource); ok {
		return res, true
	}

	if m, ok := i.(map[string]interface{}); ok {
		return m, true
	}

	m, ok := i.(map[interface{}]interface{})
	if !ok {
		return nil, false
	}

	newRes := make(Resource)
	for k, v := range m {
		s, ok := k.(string)
		if !ok {
			continue
		}
		newRes[s] = v
	}
	return newRes, true
}

func ifaceToStr(v interface{}) (string, bool) {
	if v == nil {
		return "", false
	}

	if s, ok := v.(string); ok {
		return s, true
	}

	if i, ok := v.(int); ok {
		return strconv.Itoa(i), true
	}

	if f, ok := v.(float64); ok {
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}

	return "", false
}

func uniqResources(resources []Resource) []Resource {
	type key struct {
		kind Kind
		name string
	}
	m := make(map[key]bool)

	out := make([]Resource, 0, len(resources))
	for _, r := range resources {
		k, err := r.kind()
		if err != nil {
			continue
		}
		if err := k.OK(); err != nil {
			continue
		}
		switch k {
		// these 3 kinds are unique, have existing state identifiable by name
		case KindBucket, KindLabel, KindVariable:
			rKey := key{kind: k, name: r.Name()}
			if m[rKey] {
				continue
			}
			m[rKey] = true
			fallthrough
		default:
			out = append(out, r)
		}
	}
	return out
}

// ParseError is the error from parsing the given package. The ParseError
// behavior provides a list of resources that failed and all validations
// that failed for that resource. A resource can multiple errors, and
// a parseErr can have multiple resources which themselves can have
// multiple validation failures.
type ParseError interface {
	ValidationErrs() []ValidationErr
}

type (
	parseErr struct {
		Resources []resourceErr
	}

	// resourceErr describes the error for a particular resource. In
	// which it may have numerous validation and association errors.
	resourceErr struct {
		Kind            string
		Idx             *int
		RootErrs        []validationErr
		AssociationErrs []validationErr
		ValidationErrs  []validationErr
	}

	validationErr struct {
		Field string
		Msg   string
		Index *int

		Nested []validationErr
	}
)

// Error implements the error interface.
func (e *parseErr) Error() string {
	var errMsg []string
	for _, ve := range e.ValidationErrs() {
		errMsg = append(errMsg, ve.Error())
	}

	return strings.Join(errMsg, "\n\t")
}

func (e *parseErr) ValidationErrs() []ValidationErr {
	var errs []ValidationErr
	for _, r := range e.Resources {

		rootErr := ValidationErr{
			Kind: r.Kind,
		}
		for _, v := range r.RootErrs {
			errs = append(errs, traverseErrs(rootErr, v)...)
		}

		rootErr.Indexes = []*int{r.Idx}
		rootErr.Fields = []string{"spec.resources"}
		for _, v := range append(r.ValidationErrs, r.AssociationErrs...) {
			errs = append(errs, traverseErrs(rootErr, v)...)
		}
	}
	return errs
}

// ValidationErr represents an error during the parsing of a package.
type ValidationErr struct {
	Kind    string   `json:"kind" yaml:"kind"`
	Fields  []string `json:"fields" yaml:"fields"`
	Indexes []*int   `json:"idxs" yaml:"idxs"`
	Reason  string   `json:"reason" yaml:"reason"`
}

func (v ValidationErr) Error() string {
	fieldPairs := make([]string, 0, len(v.Fields))
	for i, idx := range v.Indexes {
		field := v.Fields[i]
		if idx == nil || *idx == -1 {
			fieldPairs = append(fieldPairs, field)
			continue
		}
		fieldPairs = append(fieldPairs, fmt.Sprintf("%s[%d]", field, *idx))
	}

	return fmt.Sprintf("kind=%s field=%s reason=%q", v.Kind, strings.Join(fieldPairs, "."), v.Reason)
}

func traverseErrs(root ValidationErr, vErr validationErr) []ValidationErr {
	root.Fields = append(root.Fields, vErr.Field)
	root.Indexes = append(root.Indexes, vErr.Index)
	if len(vErr.Nested) == 0 {
		root.Reason = vErr.Msg
		return []ValidationErr{root}
	}

	var errs []ValidationErr
	for _, n := range vErr.Nested {
		errs = append(errs, traverseErrs(root, n)...)
	}
	return errs
}

func (e *parseErr) append(errs ...resourceErr) {
	e.Resources = append(e.Resources, errs...)
}

// IsParseErr inspects a given error to determine if it is
// a parseErr. If a parseErr it is, it will return it along
// with the confirmation boolean. If the error is not a parseErr
// it will return nil values for the parseErr, making it unsafe
// to use.
func IsParseErr(err error) bool {
	_, ok := err.(*parseErr)
	return ok
}
