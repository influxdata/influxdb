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
func Parse(encoding Encoding, readerFn ReaderFn) (*Pkg, error) {
	r, err := readerFn()
	if err != nil {
		return nil, err
	}

	switch encoding {
	case EncodingYAML:
		return parseYAML(r)
	case EncodingJSON:
		return parseJSON(r)
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

// FromBytes provides a reader from a byte array.
func FromBytes(b []byte) ReaderFn {
	return func() (io.Reader, error) {
		return bytes.NewReader(b), nil
	}
}

// FromString parses a pkg from a raw string value. This is very useful
// in tests.
func FromString(s string) ReaderFn {
	return func() (io.Reader, error) {
		return strings.NewReader(s), nil
	}
}

func parseYAML(r io.Reader) (*Pkg, error) {
	return parse(yaml.NewDecoder(r))
}

func parseJSON(r io.Reader) (*Pkg, error) {
	return parse(json.NewDecoder(r))
}

type decoder interface {
	Decode(interface{}) error
}

func parse(dec decoder) (*Pkg, error) {
	var pkg Pkg
	if err := dec.Decode(&pkg); err != nil {
		return nil, err
	}

	if err := pkg.Validate(); err != nil {
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
	Kind       string   `yaml:"kind" json:"kind"`
	Metadata   Metadata `yaml:"meta" json:"meta"`
	Spec       struct {
		Resources []Resource `yaml:"resources" json:"resources"`
	} `yaml:"spec" json:"spec"`

	mLabels     map[string]*label
	mBuckets    map[string]*bucket
	mDashboards map[string]*dashboard

	isVerified bool
}

// Summary returns a package Summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Pkg) Summary() Summary {
	var sum Summary

	for _, l := range p.labels() {
		sum.Labels = append(sum.Labels, l.summarize())
	}

	for _, b := range p.buckets() {
		sum.Buckets = append(sum.Buckets, b.summarize())
	}

	for _, d := range p.dashboards() {
		sum.Dashboards = append(sum.Dashboards, d.summarize())
	}

	for _, m := range p.labelMappings() {
		sum.LabelMappings = append(sum.LabelMappings, SummaryLabelMapping{
			ResourceName: m.ResourceName,
			LabelName:    m.LabelName,
			LabelMapping: m.LabelMapping,
		})
	}

	return sum
}

// Validate will graph all resources and validate every thing is in a useful form.
func (p *Pkg) Validate() error {
	setupFns := []func() error{
		p.validMetadata,
		p.validResources,
		p.graphResources,
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return err
		}
	}
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
	var failures []*failure
	if p.APIVersion != "0.1.0" {
		failures = append(failures, &failure{
			Field: "apiVersion",
			Msg:   "must be version 0.1.0",
		})
	}

	mKind := kind(strings.TrimSpace(strings.ToLower(p.Kind)))
	if mKind != kindPackage {
		failures = append(failures, &failure{
			Field: "kind",
			Msg:   `must be of kind "Package"`,
		})
	}

	if p.Metadata.Version == "" {
		failures = append(failures, &failure{
			Field: "pkgVersion",
			Msg:   "version is required",
		})
	}

	if p.Metadata.Name == "" {
		failures = append(failures, &failure{
			Field: "pkgName",
			Msg:   "must be at least 1 char",
		})
	}

	if len(failures) == 0 {
		return nil
	}

	res := errResource{
		Kind: "Package",
		Idx:  -1,
	}
	for _, f := range failures {
		res.ValidationFails = append(res.ValidationFails, struct {
			Field string
			Msg   string
		}{
			Field: f.Field,
			Msg:   f.Msg,
		})
	}
	var err ParseErr
	err.append(res)
	return &err
}

func (p *Pkg) validResources() error {
	if len(p.Spec.Resources) > 0 {
		return nil
	}

	res := errResource{
		Kind: "Package",
		Idx:  -1,
	}
	res.ValidationFails = append(res.ValidationFails, struct {
		Field string
		Msg   string
	}{Field: "resources", Msg: "at least 1 resource must be provided"})
	var err ParseErr
	err.append(res)
	return &err
}

func (p *Pkg) graphResources() error {
	graphFns := []func() error{
		// labels are first to validate associations with other resources
		p.graphLabels,
		p.graphBuckets,
		p.graphDashboards,
	}

	for _, fn := range graphFns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pkg) graphBuckets() error {
	p.mBuckets = make(map[string]*bucket)
	return p.eachResource(kindBucket, func(r Resource) []failure {
		if r.Name() == "" {
			return []failure{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		if _, ok := p.mBuckets[r.Name()]; ok {
			return []failure{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		bkt := &bucket{
			Name:            r.Name(),
			Description:     r.stringShort("description"),
			RetentionPeriod: r.duration("retention_period"),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			bkt.labels = append(bkt.labels, l)
			p.mLabels[l.Name].setBucketMapping(bkt, false)
			return nil
		})
		if len(failures) > 0 {
			return failures
		}
		sort.Slice(bkt.labels, func(i, j int) bool {
			return bkt.labels[i].Name < bkt.labels[j].Name
		})

		p.mBuckets[r.Name()] = bkt

		return failures
	})
}

func (p *Pkg) graphLabels() error {
	p.mLabels = make(map[string]*label)
	return p.eachResource(kindLabel, func(r Resource) []failure {
		if r.Name() == "" {
			return []failure{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		if _, ok := p.mLabels[r.Name()]; ok {
			return []failure{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}
		p.mLabels[r.Name()] = &label{
			Name:        r.Name(),
			Color:       r.stringShort("color"),
			Description: r.stringShort("description"),
		}

		return nil
	})
}

func (p *Pkg) graphDashboards() error {
	p.mDashboards = make(map[string]*dashboard)
	return p.eachResource(kindDashboard, func(r Resource) []failure {
		if r.Name() == "" {
			return []failure{{
				Field: "name",
				Msg:   "must be a string of at least 2 chars in length",
			}}
		}

		if _, ok := p.mDashboards[r.Name()]; ok {
			return []failure{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		dash := &dashboard{
			Name:        r.Name(),
			Description: r.stringShort("description"),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			dash.labels = append(dash.labels, l)
			p.mLabels[l.Name].setDashboardMapping(dash)
			return nil
		})
		sort.Slice(dash.labels, func(i, j int) bool {
			return dash.labels[i].Name < dash.labels[j].Name
		})

		for i, cr := range r.slcResource("charts") {
			ch, fails := parseChart(cr)
			if fails != nil {
				for _, f := range fails {
					failures = append(failures, failure{
						Field: fmt.Sprintf("charts[%d].%s", i, f.Field),
						Msg:   f.Msg,
					})
				}
				continue
			}
			dash.Charts = append(dash.Charts, ch)
		}

		if len(failures) > 0 {
			return failures
		}

		p.mDashboards[r.Name()] = dash

		return nil
	})
}

func (p *Pkg) eachResource(resourceKind kind, fn func(r Resource) []failure) error {
	var parseErr ParseErr
	for i, r := range p.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			parseErr.append(errResource{
				Kind: k.String(),
				Idx:  i,
				ValidationFails: []struct {
					Field string
					Msg   string
				}{
					{
						Field: "kind",
						Msg:   err.Error(),
					},
				},
			})
			continue
		}
		if k != resourceKind {
			continue
		}

		if failures := fn(r); failures != nil {
			err := errResource{
				Kind: resourceKind.String(),
				Idx:  i,
			}
			for _, f := range failures {
				if f.fromAssociation {
					err.AssociationFails = append(err.AssociationFails, struct {
						Field string
						Msg   string
						Index int
					}{Field: f.Field, Msg: f.Msg, Index: f.assIndex})
					continue
				}
				err.ValidationFails = append(err.ValidationFails, struct {
					Field string
					Msg   string
				}{Field: f.Field, Msg: f.Msg})
			}
			parseErr.append(err)
		}
	}

	if len(parseErr.Resources) > 0 {
		return &parseErr
	}
	return nil
}

func (p *Pkg) parseNestedLabels(r Resource, fn func(lb *label) error) []failure {
	nestedLabels := make(map[string]*label)

	var failures []failure
	for i, nr := range r.nestedAssociations() {
		fail := p.parseNestedLabel(i, nr, func(l *label) error {
			if _, ok := nestedLabels[l.Name]; ok {
				return fmt.Errorf("duplicate nested label: %q", l.Name)
			}
			nestedLabels[l.Name] = l

			return fn(l)
		})
		if fail != nil {
			failures = append(failures, *fail)
		}
	}

	return failures
}

func (p *Pkg) parseNestedLabel(idx int, nr Resource, fn func(lb *label) error) *failure {
	k, err := nr.kind()
	if err != nil {
		return &failure{
			Field:           "kind",
			Msg:             err.Error(),
			fromAssociation: true,
			assIndex:        idx,
		}
	}
	if k != kindLabel {
		return nil
	}

	lb, found := p.mLabels[nr.Name()]
	if !found {
		return &failure{
			Field:           "associations",
			Msg:             fmt.Sprintf("label %q does not exist in pkg", nr.Name()),
			fromAssociation: true,
			assIndex:        idx,
		}
	}

	if err := fn(lb); err != nil {
		return &failure{
			Field:           "associations",
			Msg:             err.Error(),
			fromAssociation: true,
			assIndex:        idx,
		}
	}
	return nil
}

func parseChart(r Resource) (chart, []failure) {
	ck, err := r.chartKind()
	if err != nil {
		return chart{}, []failure{{
			Field: "kind",
			Msg:   err.Error(),
		}}
	}

	c := chart{
		Kind:        ck,
		Name:        r.Name(),
		Prefix:      r.stringShort("prefix"),
		Suffix:      r.stringShort("suffix"),
		Note:        r.stringShort("note"),
		NoteOnEmpty: r.boolShort("noteOnEmpty"),
		Shade:       r.boolShort("shade"),
		XCol:        r.stringShort("xCol"),
		YCol:        r.stringShort("yCol"),
		XPos:        r.intShort("xPos"),
		YPos:        r.intShort("yPos"),
		Height:      r.intShort("height"),
		Width:       r.intShort("width"),
		Geom:        r.stringShort("geom"),
	}

	if leg, ok := ifaceMapToResource(r["legend"]); ok {
		c.Legend.Type = leg.stringShort("type")
		c.Legend.Orientation = leg.stringShort("orientation")
	}

	if dp, ok := r.int("decimalPlaces"); ok {
		c.EnforceDecimals = true
		c.DecimalPlaces = dp
	}

	var failures []failure
	for _, rq := range r.slcResource("queries") {
		c.Queries = append(c.Queries, query{
			Query: strings.TrimSpace(rq.stringShort("query")),
		})
	}

	for _, rc := range r.slcResource("colors") {
		c.Colors = append(c.Colors, &color{
			id:    influxdb.ID(int(time.Now().UnixNano())).String(),
			Name:  rc.Name(),
			Type:  rc.stringShort("type"),
			Hex:   rc.stringShort("hex"),
			Value: rc.float64Short("value"),
		})
	}

	for _, ra := range r.slcResource("axes") {
		c.Axes = append(c.Axes, axis{
			Base:   ra.stringShort("base"),
			Label:  ra.stringShort("label"),
			Name:   ra.Name(),
			Prefix: ra.stringShort("prefix"),
			Scale:  ra.stringShort("scale"),
			Suffix: ra.stringShort("suffix"),
		})
	}

	if fails := c.validProperties(); len(fails) > 0 {
		failures = append(failures, fails...)
	}

	if len(failures) > 0 {
		return chart{}, failures
	}

	return c, nil
}

// Resource is a pkger Resource kind. It can be one of any of
// available kinds that are supported.
type Resource map[string]interface{}

func (r Resource) Name() string {
	return strings.TrimSpace(r.stringShort("name"))
}

func (r Resource) kind() (kind, error) {
	resKind, ok := r.string("kind")
	if !ok {
		return kindUnknown, errors.New("no kind provided")
	}

	newKind := kind(strings.TrimSpace(strings.ToLower(resKind)))
	if newKind == kindUnknown {
		return kindUnknown, errors.New("invalid kind")
	}

	return newKind, nil
}

func (r Resource) chartKind() (ChartKind, error) {
	ck, _ := r.kind()
	chartKind := ChartKind(ck)
	if !chartKind.ok() {
		return ChartKindUnknown, errors.New("invalid chart kind provided: " + string(chartKind))
	}
	return chartKind, nil
}

func (r Resource) nestedAssociations() []Resource {
	v, ok := r["associations"]
	if !ok {
		return nil
	}

	ifaces, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var resources []Resource
	for _, iface := range ifaces {
		newRes, ok := ifaceMapToResource(iface)
		if !ok {
			continue
		}
		resources = append(resources, newRes)
	}

	return resources
}

func (r Resource) bool(key string) (bool, bool) {
	b, ok := r[key].(bool)
	return b, ok
}

func (r Resource) boolShort(key string) bool {
	b, _ := r.bool(key)
	return b
}

func (r Resource) duration(key string) time.Duration {
	dur, _ := time.ParseDuration(r.stringShort(key))
	return dur
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
	if s, ok := r[key].(string); ok {
		return s, true
	}

	if i, ok := r[key].(int); ok {
		return strconv.Itoa(i), true
	}

	return "", false
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

	iFaceSlc, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var newResources []Resource
	for _, iFace := range iFaceSlc {
		r, ok := ifaceMapToResource(iFace)
		if !ok {
			continue
		}
		newResources = append(newResources, r)
	}

	return newResources
}

func ifaceMapToResource(i interface{}) (Resource, bool) {
	res, ok := i.(Resource)
	if ok {
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

// ParseErr is a error from parsing the given package. The ParseErr
// provides a list of resources that failed and all validations
// that failed for that resource. A resource can multiple errors,
// and a ParseErr can have multiple resources which themselves can
// have multiple validation failures.
type ParseErr struct {
	Resources []struct {
		Kind            string
		Idx             int
		ValidationFails []struct {
			Field string
			Msg   string
		}
		AssociationFails []struct {
			Field string
			Msg   string
			Index int
		}
	}
}

// Error implements the error interface.
func (e *ParseErr) Error() string {
	var errMsg []string
	for _, r := range e.Resources {
		resIndex := strconv.Itoa(r.Idx)
		if r.Idx == -1 {
			resIndex = "root"
		}
		err := fmt.Sprintf("resource_index=%s resource_kind=%q", resIndex, r.Kind)
		errMsg = append(errMsg, err)
		for _, f := range r.ValidationFails {
			// for time being we go to new line and indent them (mainly for CLI)
			// other callers (i.e. HTTP client) can inspect the resource and print it out
			// or we provide a format option of sorts. We'll see
			errMsg = append(errMsg, fmt.Sprintf("\terr_type=%q field=%q reason=%q", "validation", f.Field, f.Msg))
		}
		for _, f := range r.AssociationFails {
			errMsg = append(errMsg, fmt.Sprintf("\terr_type=%q field=%q association_index=%d reason=%q", "association", f.Field, f.Index, f.Msg))
		}
	}

	return strings.Join(errMsg, "\n")
}

func (e *ParseErr) append(err errResource) {
	e.Resources = append(e.Resources, err)
}

// IsParseErr inspects a given error to determine if it is
// a ParseErr. If a ParseErr it is, it will return it along
// with the confirmation boolean. If the error is not a ParseErr
// it will return nil values for the ParseErr, making it unsafe
// to use.
func IsParseErr(err error) (*ParseErr, bool) {
	pErr, ok := err.(*ParseErr)
	return pErr, ok
}

type errResource struct {
	Kind            string
	Idx             int
	ValidationFails []struct {
		Field string
		Msg   string
	}
	AssociationFails []struct {
		Field string
		Msg   string
		Index int
	}
}

type failure struct {
	Field, Msg      string
	fromAssociation bool
	assIndex        int
}
