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

	mBuckets map[string]*bucket
}

// Summary returns a package summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (m *Pkg) Summary() Summary {
	var sum Summary

	type bkt struct {
		influxdb.Bucket
		Associations []influxdb.Label
	}
	for _, b := range m.mBuckets {
		sum.Buckets = append(sum.Buckets, bkt{
			Bucket: influxdb.Bucket{
				ID:              b.ID,
				OrgID:           b.OrgID,
				Name:            b.Name,
				Description:     b.Description,
				RetentionPeriod: b.RetentionPeriod,
			},
		})
	}
	sort.Slice(sum.Buckets, func(i, j int) bool {
		return sum.Buckets[i].Name < sum.Buckets[j].Name
	})

	return sum
}

func (m *Pkg) buckets() []*bucket {
	buckets := make([]*bucket, 0, len(m.mBuckets))
	for _, b := range m.mBuckets {
		buckets = append(buckets, b)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets
}

func (m *Pkg) validMetadata() error {
	var failures []*failure
	if m.APIVersion != "0.1.0" {
		failures = append(failures, &failure{
			field: "apiVersion",
			msg:   "must be version 1.0.0",
		})
	}

	mKind := kind(strings.TrimSpace(strings.ToLower(m.Kind)))
	if mKind != kindPackage {
		failures = append(failures, &failure{
			field: "kind",
			msg:   `must be of kind "Package"`,
		})
	}

	if m.Metadata.Version == "" {
		failures = append(failures, &failure{
			field: "pkgVersion",
			msg:   "version is required",
		})
	}

	if m.Metadata.Name == "" {
		failures = append(failures, &failure{
			field: "pkgName",
			msg:   "must be at least 1 char",
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
		res.ValidationFailures = append(res.ValidationFailures, struct {
			Field string
			Msg   string
		}{
			Field: f.field,
			Msg:   f.msg,
		})
	}
	var err ParseErr
	err.append(res)
	return &err
}

func (m *Pkg) graphResources() error {
	graphFns := []func() error{
		m.graphBuckets,
	}

	for _, fn := range graphFns {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (m *Pkg) graphBuckets() error {
	m.mBuckets = make(map[string]*bucket)
	return m.eachResource(kindBucket, func(r Resource) *failure {
		if r.Name() == "" {
			return &failure{
				field: "name",
				msg:   "must be a string of at least 2 chars in length",
			}
		}

		if _, ok := m.mBuckets[r.Name()]; ok {
			return &failure{
				field: "name",
				msg:   "duplicate name: " + r.Name(),
			}
		}
		m.mBuckets[r.Name()] = &bucket{
			Name:            r.Name(),
			Description:     r.stringShort("description"),
			RetentionPeriod: r.duration("retention_period"),
		}

		return nil
	})
}

func (m *Pkg) eachResource(resourceKind kind, fn func(r Resource) *failure) error {
	var parseErr ParseErr
	for i, r := range m.Spec.Resources {
		k, err := r.kind()
		if err != nil {
			parseErr.append(errResource{
				Type: k.String(),
				Idx:  i,
				ValidationFailures: []struct {
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

		if errAt := fn(r); errAt != nil {
			parseErr.append(errResource{
				Type: resourceKind.String(),
				Idx:  i,
				ValidationFailures: []struct {
					Field string
					Msg   string
				}{
					{
						Field: errAt.field,
						Msg:   errAt.msg,
					},
				},
			})
		}
	}

	if len(parseErr.Resources) > 0 {
		return &parseErr
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
	return r.stringShort("name")
}

func (r Resource) nestedResources() []Resource {
	v, ok := r["resources"]
	if !ok {
		return nil
	}

	ifaces, ok := v.([]interface{})
	if !ok {
		return nil
	}

	var resources []Resource
	for _, res := range ifaces {
		newRes, ok := ifaceMapToResource(res)
		if !ok {
			continue
		}
		resources = append(resources, newRes)
	}

	return resources
}

func (r Resource) bool(key string) bool {
	b, _ := r[key].(bool)
	return b
}

func (r Resource) duration(key string) time.Duration {
	dur, _ := time.ParseDuration(r.stringShort(key))
	return dur
}

func (r Resource) float64(key string) float64 {
	i, ok := r[key].(float64)
	if !ok {
		return 0
	}
	return i
}

func (r Resource) intShort(key string) int {
	i, _ := r.int(key)
	return i
}

func (r Resource) int(key string) (int, bool) {
	i, ok := r[key].(int)
	if ok {
		return i, true
	}

	s, ok := r[key].(string)
	if !ok {
		return 0, false
	}

	i, err := strconv.Atoi(s)
	return i, err == nil
}

func (r Resource) string(key string) (string, bool) {
	s, ok := r[key].(string)
	return s, ok
}

func (r Resource) stringShort(key string) string {
	s, _ := r.string(key)
	return s
}

func (r Resource) slcStr(key string) ([]string, bool) {
	v, ok := r[key].([]interface{})
	if !ok {
		return nil, false
	}

	out := make([]string, 0, len(v))
	for _, iface := range v {
		s, ok := iface.(string)
		if ok {
			out = append(out, s)
		}
	}
	return out, true
}

func ifaceMapToResource(i interface{}) (Resource, bool) {
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
		Type               string
		Idx                int
		ValidationFailures []struct {
			Field string
			Msg   string
		}
	}
}

// Error implements the error interface.
func (e *ParseErr) Error() string {
	var errMsg []string
	for _, r := range e.Resources {
		resIndex := fmt.Sprintf("%d", r.Idx)
		if r.Idx == -1 {
			resIndex = "root"
		}
		err := fmt.Sprintf("resource_index=%s resource_type=%q", r.Type, resIndex)
		errMsg = append(errMsg, err)
		for _, f := range r.ValidationFailures {
			// for time being we go to new line and indent them (mainly for CLI)
			// other callers (i.e. HTTP client) can inspect the resource and print it out
			// or we provide a format option of sorts. We'll see
			errMsg = append(errMsg, fmt.Sprintf("\tfield=%q reason=%q", f.Field, f.Msg))
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
	Type               string
	Idx                int
	ValidationFailures []struct {
		Field string
		Msg   string
	}
}

type failure struct {
	field, msg string
}
