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
	EncodingYAML Encoding = iota + 1
	EncodingJSON
)

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
		// TODO: fixup error
		return nil, errors.New("invalid encoding provided")
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
	err := dec.Decode(&pkg)
	if err != nil {
		return nil, err
	}

	setupFns := []func() error{
		pkg.validMetadata,
		pkg.graphResources,
	}

	for _, fn := range setupFns {
		if err := fn(); err != nil {
			return nil, err
		}
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

	mLabels  map[string]*label
	mBuckets map[string]*bucket

	isVerified bool
}

// Summary returns a package summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Pkg) Summary() Summary {
	var sum Summary

	for _, l := range p.mLabels {
		sum.Labels = append(sum.Labels, l.summarize())
	}
	sort.Slice(sum.Labels, func(i, j int) bool {
		return sum.Labels[i].Name < sum.Labels[j].Name
	})

	for _, b := range p.mBuckets {
		sum.Buckets = append(sum.Buckets, b.summarize())
	}
	sort.Slice(sum.Buckets, func(i, j int) bool {
		return sum.Buckets[i].Name < sum.Buckets[j].Name
	})

	for _, m := range p.labelMappings() {
		sum.LabelMappings = append(sum.LabelMappings, struct {
			ResourceName string
			LabelName    string
			influxdb.LabelMapping
		}{
			ResourceName: m.ResourceName,
			LabelName:    m.LabelName,
			LabelMapping: m.LabelMapping,
		})
	}
	// sort by res type ASC, then res name ASC, then label name ASC
	sort.Slice(sum.LabelMappings, func(i, j int) bool {
		n, m := sum.LabelMappings[i], sum.LabelMappings[j]
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

	return sum
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

// labelMappings returns the mappings that will be created for
// valid pairs of labels and resources of which all have IDs.
// If a resource does not exist yet, a label mapping will not
// be returned for it.
func (p *Pkg) labelMappings() []struct {
	exists       bool
	ResourceName string
	LabelName    string
	influxdb.LabelMapping
} {
	var mappings []struct {
		exists       bool
		ResourceName string
		LabelName    string
		influxdb.LabelMapping
	}
	for _, l := range p.mLabels {
		mappings = append(mappings, l.mappingSummary()...)
	}

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
		Type: "Package",
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

func (p *Pkg) graphResources() error {
	graphFns := []func() error{
		// labels are first to validate associations with other resources
		p.graphLabels,
		p.graphBuckets,
	}

	for _, fn := range graphFns {
		if err := fn(); err != nil {
			return err
		}
	}

	// TODO: make sure a resource was created....

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

		nestedLabels := make(map[string]*label)
		var failures []failure
		for i, nr := range r.nestedAssociations() {
			fail := p.processNestedLabel(i, nr, func(l *label) error {
				if _, ok := nestedLabels[l.Name]; ok {
					return fmt.Errorf("duplicate nested label: %q", l.Name)
				}
				nestedLabels[l.Name] = l
				bkt.labels = append(bkt.labels, l)
				return nil
			})
			if fail != nil {
				failures = append(failures, *fail)
			}
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

func (p *Pkg) eachResource(resourceKind kind, fn func(r Resource) []failure) error {
	var parseErr ParseErr
	for i, r := range p.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			parseErr.append(errResource{
				Type: k.String(),
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
				Type: resourceKind.String(),
				Idx:  i,
			}
			for _, f := range failures {
				if f.fromAssociation {
					err.AssociationFails = append(err.AssociationFails, struct {
						Field string
						Msg   string
						Index int
					}{Field: f.Field, Msg: f.Msg, Index: f.Index})
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

func (p *Pkg) processNestedLabel(idx int, nr Resource, fn func(lb *label) error) *failure {
	k, err := nr.kind()
	if err != nil {
		return &failure{
			Field:           "kind",
			Msg:             err.Error(),
			fromAssociation: true,
			Index:           idx,
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
			Index:           idx,
		}
	}

	if err := fn(lb); err != nil {
		return &failure{
			Field:           "associations",
			Msg:             err.Error(),
			fromAssociation: true,
			Index:           idx,
		}
	}
	return nil
}

// Resource is a pkger Resource kind. It can be one of any of
// available kinds that are supported.
type Resource map[string]interface{}

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

func (r Resource) Name() string {
	return strings.TrimSpace(r.stringShort("name"))
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

func (r Resource) duration(key string) time.Duration {
	dur, _ := time.ParseDuration(r.stringShort(key))
	return dur
}

func (r Resource) string(key string) (string, bool) {
	s, ok := r[key].(string)
	return s, ok
}

func (r Resource) stringShort(key string) string {
	s, _ := r.string(key)
	return s
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
		Type            string
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
		err := fmt.Sprintf("resource_index=%s resource_type=%q", r.Type, resIndex)
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
	Type            string
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
	Index           int
}
