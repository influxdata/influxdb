package pkger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/edit"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/influxdb/v2/pkg/jsonnet"
	"github.com/influxdata/influxdb/v2/task/options"
	"gopkg.in/yaml.v3"
)

type (
	// ReaderFn is used for functional inputs to abstract the individual
	// entrypoints for the reader itself.
	ReaderFn func() (r io.Reader, source string, err error)

	// Encoder is an encodes a type.
	Encoder interface {
		Encode(v interface{}) error
	}

	// Encoding describes the encoding for the raw package data. The
	// encoding determines how the raw data is parsed.
	Encoding int
)

// encoding types
const (
	EncodingUnknown Encoding = iota
	EncodingJSON
	EncodingJsonnet
	EncodingSource // EncodingSource draws the encoding type by inferring it from the source.
	EncodingYAML
)

// String provides the string representation of the encoding.
func (e Encoding) String() string {
	switch e {
	case EncodingJSON:
		return "json"
	case EncodingJsonnet:
		return "jsonnet"
	case EncodingSource:
		return "source"
	case EncodingYAML:
		return "yaml"
	default:
		return "unknown"
	}
}

// ErrInvalidEncoding indicates the encoding is invalid type for the parser.
var ErrInvalidEncoding = errors.New("invalid encoding provided")

// Parse parses a pkg defined by the encoding and readerFns. As of writing this
// we can parse both a YAML, JSON, and Jsonnet formats of the Template model.
func Parse(encoding Encoding, readerFn ReaderFn, opts ...ValidateOptFn) (*Template, error) {
	r, source, err := readerFn()
	if err != nil {
		return nil, err
	}

	var pkgFn func(io.Reader, ...ValidateOptFn) (*Template, error)
	switch encoding {
	case EncodingJSON:
		pkgFn = parseJSON
	case EncodingJsonnet:
		pkgFn = parseJsonnet
	case EncodingSource:
		pkgFn = parseSource
	case EncodingYAML:
		pkgFn = parseYAML
	default:
		return nil, ErrInvalidEncoding
	}

	pkg, err := pkgFn(r, opts...)
	if err != nil {
		return nil, err
	}
	pkg.sources = []string{source}

	return pkg, nil
}

// FromFile reads a file from disk and provides a reader from it.
func FromFile(filePath string) ReaderFn {
	return func() (io.Reader, string, error) {
		u, err := url.Parse(filePath)
		if err != nil {
			return nil, filePath, &errors2.Error{
				Code: errors2.EInvalid,
				Msg:  "invalid filepath provided",
				Err:  err,
			}
		}
		if u.Scheme == "" {
			u.Scheme = "file"
		}

		// not using os.Open to avoid having to deal with closing the file in here
		b, err := ioutil.ReadFile(u.Path)
		if err != nil {
			return nil, filePath, err
		}

		return bytes.NewBuffer(b), u.String(), nil
	}
}

// FromReader simply passes the reader along. Useful when consuming
// this from an HTTP request body. There are a number of other useful
// places for this functional input.
func FromReader(r io.Reader, sources ...string) ReaderFn {
	return func() (io.Reader, string, error) {
		source := "byte stream"
		if len(sources) > 0 {
			source = formatSources(sources)
		}
		return r, source, nil
	}
}

// FromString parses a pkg from a raw string value. This is very useful
// in tests.
func FromString(s string) ReaderFn {
	return func() (io.Reader, string, error) {
		return strings.NewReader(s), "string", nil
	}
}

var defaultHTTPClient = &http.Client{
	Timeout: time.Minute,
}

// FromHTTPRequest parses a pkg from the request body of a HTTP request. This is
// very useful when using packages that are hosted..
func FromHTTPRequest(addr string) ReaderFn {
	return func() (io.Reader, string, error) {
		resp, err := defaultHTTPClient.Get(normalizeGithubURLToContent(addr))
		if err != nil {
			return nil, addr, err
		}
		defer resp.Body.Close()

		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return nil, addr, err
		}

		if resp.StatusCode/100 != 2 {
			return nil, addr, fmt.Errorf(
				"bad response: address=%s status_code=%d body=%q",
				addr, resp.StatusCode, strings.TrimSpace(buf.String()),
			)
		}

		return &buf, addr, nil
	}
}

const (
	githubRawContentHost = "raw.githubusercontent.com"
	githubHost           = "github.com"
)

func normalizeGithubURLToContent(addr string) string {
	u, err := url.Parse(addr)
	if err != nil {
		return addr
	}

	if u.Host == githubHost {
		switch path.Ext(u.Path) {
		case ".yaml", ".yml", ".json", ".jsonnet":
		default:
			return u.String()
		}

		parts := strings.Split(u.Path, "/")
		if len(parts) < 4 {
			return u.String()
		}
		u.Host = githubRawContentHost
		u.Path = path.Join(append(parts[:3], parts[4:]...)...)
	}

	return u.String()
}

func parseJSON(r io.Reader, opts ...ValidateOptFn) (*Template, error) {
	return parse(json.NewDecoder(r), opts...)
}

func parseJsonnet(r io.Reader, opts ...ValidateOptFn) (*Template, error) {
	return parse(jsonnet.NewDecoder(r), opts...)
}

func parseSource(r io.Reader, opts ...ValidateOptFn) (*Template, error) {
	var b []byte
	if byter, ok := r.(interface{ Bytes() []byte }); ok {
		b = byter.Bytes()
	} else {
		bb, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode pkg source: %s", err)
		}
		b = bb
	}

	contentType := http.DetectContentType(b[:512])
	switch {
	case strings.Contains(contentType, "jsonnet"):
		// highly unlikely to fall in here with supported content type detection as is
		return parseJsonnet(bytes.NewReader(b), opts...)
	case strings.Contains(contentType, "json"):
		return parseJSON(bytes.NewReader(b), opts...)
	case strings.Contains(contentType, "yaml"),
		strings.Contains(contentType, "yml"):
		return parseYAML(bytes.NewReader(b), opts...)
	default:
		return parseYAML(bytes.NewReader(b), opts...)
	}
}

func parseYAML(r io.Reader, opts ...ValidateOptFn) (*Template, error) {
	dec := yaml.NewDecoder(r)

	var pkg Template
	for {
		// forced to use this for loop b/c the yaml dependency does not
		// decode multi documents.
		var k Object
		err := dec.Decode(&k)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		pkg.Objects = append(pkg.Objects, k)
	}

	if err := pkg.Validate(opts...); err != nil {
		return nil, err
	}

	return &pkg, nil
}

type decoder interface {
	Decode(interface{}) error
}

func parse(dec decoder, opts ...ValidateOptFn) (*Template, error) {
	var pkg Template
	if err := dec.Decode(&pkg.Objects); err != nil {
		return nil, err
	}

	if err := pkg.Validate(opts...); err != nil {
		return nil, err
	}

	return &pkg, nil
}

// Object describes the metadata and raw spec for an entity of a package kind.
type Object struct {
	APIVersion string   `json:"apiVersion" yaml:"apiVersion"`
	Kind       Kind     `json:"kind" yaml:"kind"`
	Metadata   Resource `json:"metadata" yaml:"metadata"`
	Spec       Resource `json:"spec" yaml:"spec"`
}

// Name returns the name of the kind.
func (k Object) Name() string {
	return k.Metadata.references(fieldName).String()
}

// ObjectAssociation is an association for an object. The supported types
// at this time are KindLabel.
type ObjectAssociation struct {
	Kind     Kind
	MetaName string
}

// AddAssociations adds an association to the object.
func (k Object) AddAssociations(associations ...ObjectAssociation) {
	if len(associations) == 0 {
		return
	}

	if k.Spec == nil {
		k.Spec = make(Resource)
	}

	existingAss := k.Spec.slcResource(fieldAssociations)
	for _, ass := range associations {
		existingAss = append(existingAss, Resource{
			fieldKind: ass.Kind,
			fieldName: ass.MetaName,
		})
	}
	sort.Slice(existingAss, func(i, j int) bool {
		iPkgName, jPkgName := existingAss[i].Name(), existingAss[j].Name()
		return iPkgName < jPkgName
	})
	if existingAss == nil {
		return
	}

	k.Spec[fieldAssociations] = existingAss
}

// SetMetadataName sets the metadata.name field.
func (k Object) SetMetadataName(name string) {
	if k.Metadata == nil {
		k.Metadata = make(Resource)
	}
	k.Metadata[fieldName] = name
}

// Template is the model for a package. The resources are more generic that one might
// expect at first glance. This was done on purpose. The way json/yaml/toml or
// w/e scripting you want to use, can have very different ways of parsing. The
// different parsers are limited for the parsers that do not come from the std
// lib (looking at you yaml/v2). This allows us to parse it and leave the matching
// to another power, the graphing of the package is handled within itself.
type Template struct {
	Objects []Object `json:"-" yaml:"-"`
	sources []string

	mLabels                map[string]*label
	mBuckets               map[string]*bucket
	mChecks                map[string]*check
	mDashboards            map[string]*dashboard
	mNotificationEndpoints map[string]*notificationEndpoint
	mNotificationRules     map[string]*notificationRule
	mTasks                 map[string]*task
	mTelegrafs             map[string]*telegraf
	mVariables             map[string]*variable

	mEnv     map[string]bool
	mEnvVals map[string]interface{}
	mSecrets map[string]bool

	isParsed bool // indicates the pkg has been parsed and all resources graphed accordingly
}

// Encode is a helper for encoding the pkg correctly.
func (p *Template) Encode(encoding Encoding) ([]byte, error) {
	if p == nil {
		panic("attempted to encode a nil Template")
	}

	var (
		buf bytes.Buffer
		err error
	)
	switch encoding {
	case EncodingJSON, EncodingJsonnet:
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "\t")
		err = enc.Encode(p.Objects)
	case EncodingYAML:
		enc := yaml.NewEncoder(&buf)
		for _, k := range p.Objects {
			if err = enc.Encode(k); err != nil {
				break
			}
		}
	default:
		return nil, ErrInvalidEncoding
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *Template) Sources() []string {
	// note: we prevent the internal field from being changed by enabling access
	// 		 to the sources via the exported method here.
	return p.sources
}

// Summary returns a package Summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Template) Summary() Summary {
	// ensure zero values for arrays aren't returned, but instead
	// we always returning an initialized slice.
	sum := Summary{
		Buckets:               []SummaryBucket{},
		Checks:                []SummaryCheck{},
		Dashboards:            []SummaryDashboard{},
		NotificationEndpoints: []SummaryNotificationEndpoint{},
		NotificationRules:     []SummaryNotificationRule{},
		Labels:                []SummaryLabel{},
		MissingEnvs:           p.missingEnvRefs(),
		MissingSecrets:        p.missingSecrets(),
		Tasks:                 []SummaryTask{},
		TelegrafConfigs:       []SummaryTelegraf{},
		Variables:             []SummaryVariable{},
	}

	for _, b := range p.buckets() {
		sum.Buckets = append(sum.Buckets, b.summarize())
	}

	for _, c := range p.checks() {
		sum.Checks = append(sum.Checks, c.summarize())
	}

	for _, d := range p.dashboards() {
		sum.Dashboards = append(sum.Dashboards, d.summarize())
	}

	for _, l := range p.labels() {
		sum.Labels = append(sum.Labels, l.summarize())
	}

	sum.LabelMappings = p.labelMappings()

	for _, n := range p.notificationEndpoints() {
		sum.NotificationEndpoints = append(sum.NotificationEndpoints, n.summarize())
	}

	for _, r := range p.notificationRules() {
		sum.NotificationRules = append(sum.NotificationRules, r.summarize())
	}

	for _, t := range p.tasks() {
		sum.Tasks = append(sum.Tasks, t.summarize())
	}

	for _, t := range p.telegrafs() {
		sum.TelegrafConfigs = append(sum.TelegrafConfigs, t.summarize())
	}

	for _, v := range p.variables() {
		sum.Variables = append(sum.Variables, v.summarize())
	}

	return sum
}

func (p *Template) applyEnvRefs(envRefs map[string]interface{}) error {
	if len(envRefs) == 0 {
		return nil
	}

	if p.mEnvVals == nil {
		p.mEnvVals = make(map[string]interface{})
	}

	for k, v := range envRefs {
		p.mEnvVals[k] = v
	}

	return p.Validate()
}

func (p *Template) applySecrets(secrets map[string]string) {
	for k := range secrets {
		p.mSecrets[k] = true
	}
}

// Contains identifies if a pkg contains a given object identified
// by its kind and metadata.Name (MetaName) field.
func (p *Template) Contains(k Kind, pkgName string) bool {
	switch k {
	case KindBucket:
		_, ok := p.mBuckets[pkgName]
		return ok
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		_, ok := p.mChecks[pkgName]
		return ok
	case KindLabel:
		_, ok := p.mLabels[pkgName]
		return ok
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		_, ok := p.mNotificationEndpoints[pkgName]
		return ok
	case KindNotificationRule:
		_, ok := p.mNotificationRules[pkgName]
		return ok
	case KindTask:
		_, ok := p.mTasks[pkgName]
		return ok
	case KindTelegraf:
		_, ok := p.mTelegrafs[pkgName]
		return ok
	case KindVariable:
		_, ok := p.mVariables[pkgName]
		return ok
	}
	return false
}

// Combine combines pkgs together. Is useful when you want to take multiple disparate pkgs
// and compile them into one to take advantage of the parser and service guarantees.
func Combine(pkgs []*Template, validationOpts ...ValidateOptFn) (*Template, error) {
	newPkg := new(Template)
	for _, p := range pkgs {
		if len(p.Objects) == 0 {
			continue
		}
		newPkg.sources = append(newPkg.sources, p.sources...)
		newPkg.Objects = append(newPkg.Objects, p.Objects...)
	}

	return newPkg, newPkg.Validate(validationOpts...)
}

type (
	validateOpt struct {
		minResources bool
		skipValidate bool
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

// ValidSkipParseError ignores the validation check from the  of resources. This
// is useful for the service Create to ignore this and allow the creation of a
// pkg without resources.
func ValidSkipParseError() ValidateOptFn {
	return func(opt *validateOpt) {
		opt.skipValidate = true
	}
}

// Validate will graph all resources and validate every thing is in a useful form.
func (p *Template) Validate(opts ...ValidateOptFn) error {
	opt := &validateOpt{minResources: true}
	for _, o := range opts {
		o(opt)
	}

	var setupFns []func() error
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

	if len(pErr.Resources) > 0 && !opt.skipValidate {
		return &pErr
	}

	p.isParsed = true
	return nil
}

func (p *Template) buckets() []*bucket {
	buckets := make([]*bucket, 0, len(p.mBuckets))
	for _, b := range p.mBuckets {
		buckets = append(buckets, b)
	}

	sort.Slice(buckets, func(i, j int) bool { return buckets[i].MetaName() < buckets[j].MetaName() })

	return buckets
}

func (p *Template) checks() []*check {
	checks := make([]*check, 0, len(p.mChecks))
	for _, c := range p.mChecks {
		checks = append(checks, c)
	}

	sort.Slice(checks, func(i, j int) bool { return checks[i].MetaName() < checks[j].MetaName() })

	return checks
}

func (p *Template) labels() []*label {
	labels := make(sortedLabels, 0, len(p.mLabels))
	for _, l := range p.mLabels {
		labels = append(labels, l)
	}

	sort.Sort(labels)

	return labels
}

func (p *Template) dashboards() []*dashboard {
	dashes := make([]*dashboard, 0, len(p.mDashboards))
	for _, d := range p.mDashboards {
		dashes = append(dashes, d)
	}
	sort.Slice(dashes, func(i, j int) bool { return dashes[i].MetaName() < dashes[j].MetaName() })
	return dashes
}

func (p *Template) notificationEndpoints() []*notificationEndpoint {
	endpoints := make([]*notificationEndpoint, 0, len(p.mNotificationEndpoints))
	for _, e := range p.mNotificationEndpoints {
		endpoints = append(endpoints, e)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		ei, ej := endpoints[i], endpoints[j]
		if ei.kind == ej.kind {
			return ei.MetaName() < ej.MetaName()
		}
		return ei.kind < ej.kind
	})
	return endpoints
}

func (p *Template) notificationRules() []*notificationRule {
	rules := make([]*notificationRule, 0, len(p.mNotificationRules))
	for _, r := range p.mNotificationRules {
		rules = append(rules, r)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].MetaName() < rules[j].MetaName() })
	return rules
}

func (p *Template) missingEnvRefs() []string {
	envRefs := make([]string, 0)
	for envRef, matching := range p.mEnv {
		if !matching {
			envRefs = append(envRefs, envRef)
		}
	}
	sort.Strings(envRefs)
	return envRefs
}

func (p *Template) missingSecrets() []string {
	secrets := make([]string, 0, len(p.mSecrets))
	for secret, foundInPlatform := range p.mSecrets {
		if foundInPlatform {
			continue
		}
		secrets = append(secrets, secret)
	}
	return secrets
}

func (p *Template) tasks() []*task {
	tasks := make([]*task, 0, len(p.mTasks))
	for _, t := range p.mTasks {
		tasks = append(tasks, t)
	}

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].MetaName() < tasks[j].MetaName() })

	return tasks
}

func (p *Template) telegrafs() []*telegraf {
	teles := make([]*telegraf, 0, len(p.mTelegrafs))
	for _, t := range p.mTelegrafs {
		t.config.Name = t.Name()
		teles = append(teles, t)
	}

	sort.Slice(teles, func(i, j int) bool { return teles[i].MetaName() < teles[j].MetaName() })

	return teles
}

func (p *Template) variables() []*variable {
	vars := make([]*variable, 0, len(p.mVariables))
	for _, v := range p.mVariables {
		vars = append(vars, v)
	}

	sort.Slice(vars, func(i, j int) bool { return vars[i].MetaName() < vars[j].MetaName() })

	return vars
}

// labelMappings returns the mappings that will be created for
// valid pairs of labels and resources of which all have IDs.
// If a resource does not exist yet, a label mapping will not
// be returned for it.
func (p *Template) labelMappings() []SummaryLabelMapping {
	labels := p.mLabels
	mappings := make([]SummaryLabelMapping, 0, len(labels))
	for _, l := range labels {
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

func (p *Template) validResources() error {
	if len(p.Objects) > 0 {
		return nil
	}

	res := resourceErr{
		Kind: KindPackage.String(),
		RootErrs: []validationErr{{
			Field: "resources",
			Msg:   "at least 1 kind must be provided",
		}},
	}
	var err parseErr
	err.append(res)
	return &err
}

func (p *Template) graphResources() error {
	p.mEnv = make(map[string]bool)
	p.mSecrets = make(map[string]bool)

	graphFns := []func() *parseErr{
		// labels are first, this is to validate associations with other resources
		p.graphLabels,
		p.graphVariables,
		p.graphBuckets,
		p.graphChecks,
		p.graphDashboards,
		p.graphNotificationEndpoints,
		p.graphNotificationRules,
		p.graphTasks,
		p.graphTelegrafs,
	}

	var pErr parseErr
	for _, fn := range graphFns {
		if err := fn(); err != nil {
			pErr.append(err.Resources...)
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

func (p *Template) graphBuckets() *parseErr {
	p.mBuckets = make(map[string]*bucket)
	tracker := p.trackNames(true)
	return p.eachResource(KindBucket, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		bkt := &bucket{
			identity:    ident,
			Description: o.Spec.stringShort(fieldDescription),
		}
		if rules, ok := o.Spec[fieldBucketRetentionRules].(retentionRules); ok {
			bkt.RetentionRules = rules
		} else {
			for _, r := range o.Spec.slcResource(fieldBucketRetentionRules) {
				bkt.RetentionRules = append(bkt.RetentionRules, retentionRule{
					Type:    r.stringShort(fieldType),
					Seconds: r.intShort(fieldRetentionRulesEverySeconds),
				})
			}
		}
		p.setRefs(bkt.name, bkt.displayName)

		failures := p.parseNestedLabels(o.Spec, func(l *label) error {
			bkt.labels = append(bkt.labels, l)
			p.mLabels[l.MetaName()].setMapping(bkt, false)
			return nil
		})
		sort.Sort(bkt.labels)

		p.mBuckets[bkt.MetaName()] = bkt

		return append(failures, bkt.valid()...)
	})
}

func (p *Template) graphLabels() *parseErr {
	p.mLabels = make(map[string]*label)
	tracker := p.trackNames(true)
	return p.eachResource(KindLabel, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		l := &label{
			identity:    ident,
			Color:       o.Spec.stringShort(fieldLabelColor),
			Description: o.Spec.stringShort(fieldDescription),
		}
		p.mLabels[l.MetaName()] = l
		p.setRefs(l.name, l.displayName)

		return l.valid()
	})
}

func (p *Template) graphChecks() *parseErr {
	p.mChecks = make(map[string]*check)
	// todo: what is the business goal wrt having unique names? (currently duplicates are allowed)
	tracker := p.trackNames(false)

	checkKinds := []struct {
		kind      Kind
		checkKind checkKind
	}{
		{kind: KindCheckThreshold, checkKind: checkKindThreshold},
		{kind: KindCheckDeadman, checkKind: checkKindDeadman},
	}
	var pErr parseErr
	for _, checkKind := range checkKinds {
		err := p.eachResource(checkKind.kind, func(o Object) []validationErr {
			ident, errs := tracker(o)
			if len(errs) > 0 {
				return errs
			}

			ch := &check{
				kind:          checkKind.checkKind,
				identity:      ident,
				description:   o.Spec.stringShort(fieldDescription),
				every:         o.Spec.durationShort(fieldEvery),
				level:         o.Spec.stringShort(fieldLevel),
				offset:        o.Spec.durationShort(fieldOffset),
				query:         strings.TrimSpace(o.Spec.stringShort(fieldQuery)),
				reportZero:    o.Spec.boolShort(fieldCheckReportZero),
				staleTime:     o.Spec.durationShort(fieldCheckStaleTime),
				status:        normStr(o.Spec.stringShort(fieldStatus)),
				statusMessage: o.Spec.stringShort(fieldCheckStatusMessageTemplate),
				timeSince:     o.Spec.durationShort(fieldCheckTimeSince),
			}
			for _, tagRes := range o.Spec.slcResource(fieldCheckTags) {
				ch.tags = append(ch.tags, struct{ k, v string }{
					k: tagRes.stringShort(fieldKey),
					v: tagRes.stringShort(fieldValue),
				})
			}
			for _, th := range o.Spec.slcResource(fieldCheckThresholds) {
				ch.thresholds = append(ch.thresholds, threshold{
					threshType: thresholdType(normStr(th.stringShort(fieldType))),
					allVals:    th.boolShort(fieldCheckAllValues),
					level:      strings.TrimSpace(strings.ToUpper(th.stringShort(fieldLevel))),
					max:        th.float64Short(fieldMax),
					min:        th.float64Short(fieldMin),
					val:        th.float64Short(fieldValue),
				})
			}

			failures := p.parseNestedLabels(o.Spec, func(l *label) error {
				ch.labels = append(ch.labels, l)
				p.mLabels[l.MetaName()].setMapping(ch, false)
				return nil
			})
			sort.Sort(ch.labels)

			p.mChecks[ch.MetaName()] = ch
			p.setRefs(ch.name, ch.displayName)
			return append(failures, ch.valid()...)
		})
		if err != nil {
			pErr.append(err.Resources...)
		}
	}
	if len(pErr.Resources) > 0 {
		return &pErr
	}
	return nil
}

func (p *Template) graphDashboards() *parseErr {
	p.mDashboards = make(map[string]*dashboard)
	tracker := p.trackNames(false)
	return p.eachResource(KindDashboard, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		dash := &dashboard{
			identity:    ident,
			Description: o.Spec.stringShort(fieldDescription),
		}

		failures := p.parseNestedLabels(o.Spec, func(l *label) error {
			dash.labels = append(dash.labels, l)
			p.mLabels[l.MetaName()].setMapping(dash, false)
			return nil
		})
		sort.Sort(dash.labels)

		for i, cr := range o.Spec.slcResource(fieldDashCharts) {
			ch, fails := p.parseChart(dash.MetaName(), i, cr)
			if fails != nil {
				failures = append(failures,
					objectValidationErr(fieldSpec, validationErr{
						Field:  fieldDashCharts,
						Index:  intPtr(i),
						Nested: fails,
					}),
				)
				continue
			}
			dash.Charts = append(dash.Charts, ch)
		}

		p.mDashboards[dash.MetaName()] = dash
		p.setRefs(dash.refs()...)

		return append(failures, dash.valid()...)
	})
}

func (p *Template) graphNotificationEndpoints() *parseErr {
	p.mNotificationEndpoints = make(map[string]*notificationEndpoint)
	tracker := p.trackNames(true)

	notificationKinds := []struct {
		kind             Kind
		notificationKind notificationEndpointKind
	}{
		{
			kind:             KindNotificationEndpointHTTP,
			notificationKind: notificationKindHTTP,
		},
		{
			kind:             KindNotificationEndpointPagerDuty,
			notificationKind: notificationKindPagerDuty,
		},
		{
			kind:             KindNotificationEndpointSlack,
			notificationKind: notificationKindSlack,
		},
	}

	var pErr parseErr
	for _, nk := range notificationKinds {
		err := p.eachResource(nk.kind, func(o Object) []validationErr {
			ident, errs := tracker(o)
			if len(errs) > 0 {
				return errs
			}

			endpoint := &notificationEndpoint{
				kind:        nk.notificationKind,
				identity:    ident,
				description: o.Spec.stringShort(fieldDescription),
				method:      strings.TrimSpace(strings.ToUpper(o.Spec.stringShort(fieldNotificationEndpointHTTPMethod))),
				httpType:    normStr(o.Spec.stringShort(fieldType)),
				password:    o.Spec.references(fieldNotificationEndpointPassword),
				routingKey:  o.Spec.references(fieldNotificationEndpointRoutingKey),
				status:      normStr(o.Spec.stringShort(fieldStatus)),
				token:       o.Spec.references(fieldNotificationEndpointToken),
				url:         o.Spec.stringShort(fieldNotificationEndpointURL),
				username:    o.Spec.references(fieldNotificationEndpointUsername),
			}
			failures := p.parseNestedLabels(o.Spec, func(l *label) error {
				endpoint.labels = append(endpoint.labels, l)
				p.mLabels[l.MetaName()].setMapping(endpoint, false)
				return nil
			})
			sort.Sort(endpoint.labels)

			p.setRefs(
				endpoint.name,
				endpoint.displayName,
				endpoint.password,
				endpoint.routingKey,
				endpoint.token,
				endpoint.username,
			)

			p.mNotificationEndpoints[endpoint.MetaName()] = endpoint
			return append(failures, endpoint.valid()...)
		})
		if err != nil {
			pErr.append(err.Resources...)
		}
	}
	if len(pErr.Resources) > 0 {
		return &pErr
	}
	return nil
}

func (p *Template) graphNotificationRules() *parseErr {
	p.mNotificationRules = make(map[string]*notificationRule)
	tracker := p.trackNames(false)
	return p.eachResource(KindNotificationRule, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		rule := &notificationRule{
			identity:     ident,
			endpointName: p.getRefWithKnownEnvs(o.Spec, fieldNotificationRuleEndpointName),
			description:  o.Spec.stringShort(fieldDescription),
			channel:      o.Spec.stringShort(fieldNotificationRuleChannel),
			every:        o.Spec.durationShort(fieldEvery),
			msgTemplate:  o.Spec.stringShort(fieldNotificationRuleMessageTemplate),
			offset:       o.Spec.durationShort(fieldOffset),
			status:       normStr(o.Spec.stringShort(fieldStatus)),
		}

		for _, sRule := range o.Spec.slcResource(fieldNotificationRuleStatusRules) {
			rule.statusRules = append(rule.statusRules, struct{ curLvl, prevLvl string }{
				curLvl:  strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRuleCurrentLevel))),
				prevLvl: strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRulePreviousLevel))),
			})
		}

		for _, tRule := range o.Spec.slcResource(fieldNotificationRuleTagRules) {
			rule.tagRules = append(rule.tagRules, struct{ k, v, op string }{
				k:  tRule.stringShort(fieldKey),
				v:  tRule.stringShort(fieldValue),
				op: normStr(tRule.stringShort(fieldOperator)),
			})
		}

		rule.associatedEndpoint = p.mNotificationEndpoints[rule.endpointName.String()]

		failures := p.parseNestedLabels(o.Spec, func(l *label) error {
			rule.labels = append(rule.labels, l)
			p.mLabels[l.MetaName()].setMapping(rule, false)
			return nil
		})
		sort.Sort(rule.labels)

		p.mNotificationRules[rule.MetaName()] = rule
		p.setRefs(rule.name, rule.displayName, rule.endpointName)
		return append(failures, rule.valid()...)
	})
}

func (p *Template) graphTasks() *parseErr {
	p.mTasks = make(map[string]*task)
	tracker := p.trackNames(false)
	return p.eachResource(KindTask, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		t := &task{
			identity:    ident,
			cron:        o.Spec.stringShort(fieldTaskCron),
			description: o.Spec.stringShort(fieldDescription),
			every:       o.Spec.durationShort(fieldEvery),
			offset:      o.Spec.durationShort(fieldOffset),
			status:      normStr(o.Spec.stringShort(fieldStatus)),
		}

		prefix := fmt.Sprintf("tasks[%s].spec", t.MetaName())
		params := o.Spec.slcResource(fieldParams)
		task := o.Spec.slcResource("task")

		var (
			err      error
			failures []validationErr
		)

		t.query, err = p.parseQuery(prefix, o.Spec.stringShort(fieldQuery), params, task)
		if err != nil {
			failures = append(failures, validationErr{
				Field: fieldQuery,
				Msg:   err.Error(),
			})
		}

		if o.APIVersion == APIVersion2 {
			for _, ref := range t.query.task {
				switch ref.EnvRef {
				case prefix + ".task.name", prefix + ".params.name":
					t.displayName = ref
				case prefix + ".task.every":
					every, ok := ref.defaultVal.(time.Duration)
					if ok {
						t.every = every
					} else {
						failures = append(failures, validationErr{
							Field: fieldTask,
							Msg:   "field every is not duration",
						})
					}
				case prefix + ".task.offset":
					offset, ok := ref.defaultVal.(time.Duration)
					if ok {
						t.offset = offset
					} else {
						failures = append(failures, validationErr{
							Field: fieldTask,
							Msg:   "field every is not duration",
						})
					}
				}
			}
		}

		failures = append(failures, p.parseNestedLabels(o.Spec, func(l *label) error {
			t.labels = append(t.labels, l)
			p.mLabels[l.MetaName()].setMapping(t, false)
			return nil
		})...)
		sort.Sort(t.labels)

		p.mTasks[t.MetaName()] = t

		p.setRefs(t.refs()...)
		return append(failures, t.valid()...)
	})
}

func (p *Template) graphTelegrafs() *parseErr {
	p.mTelegrafs = make(map[string]*telegraf)
	tracker := p.trackNames(false)
	return p.eachResource(KindTelegraf, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		tele := &telegraf{
			identity: ident,
		}
		tele.config.Config = o.Spec.stringShort(fieldTelegrafConfig)
		tele.config.Description = o.Spec.stringShort(fieldDescription)

		failures := p.parseNestedLabels(o.Spec, func(l *label) error {
			tele.labels = append(tele.labels, l)
			p.mLabels[l.MetaName()].setMapping(tele, false)
			return nil
		})
		sort.Sort(tele.labels)

		p.mTelegrafs[tele.MetaName()] = tele
		p.setRefs(tele.name, tele.displayName)

		return append(failures, tele.valid()...)
	})
}

func (p *Template) graphVariables() *parseErr {
	p.mVariables = make(map[string]*variable)
	tracker := p.trackNames(true)
	return p.eachResource(KindVariable, func(o Object) []validationErr {
		ident, errs := tracker(o)
		if len(errs) > 0 {
			return errs
		}

		newVar := &variable{
			identity:    ident,
			Description: o.Spec.stringShort(fieldDescription),
			Type:        normStr(o.Spec.stringShort(fieldType)),
			Query:       strings.TrimSpace(o.Spec.stringShort(fieldQuery)),
			Language:    normStr(o.Spec.stringShort(fieldLanguage)),
			ConstValues: o.Spec.slcStr(fieldValues),
			MapValues:   o.Spec.mapStrStr(fieldValues),
		}

		if iSelected, ok := o.Spec[fieldVariableSelected].([]interface{}); ok {
			for _, res := range iSelected {
				newVar.selected = append(newVar.selected, ifaceToReference(res))
			}
		}

		failures := p.parseNestedLabels(o.Spec, func(l *label) error {
			newVar.labels = append(newVar.labels, l)
			p.mLabels[l.MetaName()].setMapping(newVar, false)
			return nil
		})
		sort.Sort(newVar.labels)

		p.mVariables[newVar.MetaName()] = newVar
		p.setRefs(newVar.name, newVar.displayName)
		p.setRefs(newVar.selected...)

		return append(failures, newVar.valid()...)
	})
}

func (p *Template) eachResource(resourceKind Kind, fn func(o Object) []validationErr) *parseErr {
	var pErr parseErr
	for i, k := range p.Objects {
		if err := k.Kind.OK(); err != nil {
			pErr.append(resourceErr{
				Kind: k.Kind.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					{
						Field: fieldKind,
						Msg:   err.Error(),
					},
				},
			})
			continue
		}
		if !k.Kind.is(resourceKind) {
			continue
		}

		if k.APIVersion != APIVersion && k.APIVersion != APIVersion2 {
			pErr.append(resourceErr{
				Kind: k.Kind.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					{
						Field: fieldAPIVersion,
						Msg:   fmt.Sprintf("invalid API version provided %q; must be 1 in [%s, %s]", k.APIVersion, APIVersion, APIVersion2),
					},
				},
			})
			continue
		}

		if errs := isDNS1123Label(k.Name()); len(errs) > 0 {
			pErr.append(resourceErr{
				Kind: k.Kind.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					objectValidationErr(fieldMetadata, validationErr{
						Field: fieldName,
						Msg:   fmt.Sprintf("name %q is invalid; %s", k.Name(), strings.Join(errs, "; ")),
					}),
				},
			})
			continue
		}

		if failures := fn(k); failures != nil {
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

func (p *Template) parseNestedLabels(r Resource, fn func(lb *label) error) []validationErr {
	nestedLabels := make(map[string]*label)

	var failures []validationErr
	for i, nr := range r.slcResource(fieldAssociations) {
		fail := p.parseNestedLabel(nr, func(l *label) error {
			if _, ok := nestedLabels[l.Name()]; ok {
				return fmt.Errorf("duplicate nested label: %q", l.Name())
			}
			nestedLabels[l.Name()] = l

			return fn(l)
		})
		if fail != nil {
			fail.Index = intPtr(i)
			failures = append(failures, *fail)
		}
	}

	return failures
}

func (p *Template) parseNestedLabel(nr Resource, fn func(lb *label) error) *validationErr {
	k, err := nr.kind()
	if err != nil {
		return &validationErr{
			Field: fieldAssociations,
			Nested: []validationErr{
				{
					Field: fieldKind,
					Msg:   err.Error(),
				},
			},
		}
	}
	if !k.is(KindLabel) {
		return nil
	}

	nameRef := p.getRefWithKnownEnvs(nr, fieldName)
	lb, found := p.mLabels[nameRef.String()]
	if !found {
		return &validationErr{
			Field: fieldAssociations,
			Msg:   fmt.Sprintf("label %q does not exist in pkg", nr.Name()),
		}
	}

	if err := fn(lb); err != nil {
		return &validationErr{
			Field: fieldAssociations,
			Msg:   err.Error(),
		}
	}
	return nil
}

func (p *Template) trackNames(resourceUniqueByName bool) func(Object) (identity, []validationErr) {
	mPkgNames := make(map[string]bool)
	uniqNames := make(map[string]bool)
	return func(o Object) (identity, []validationErr) {
		nameRef := p.getRefWithKnownEnvs(o.Metadata, fieldName)
		if mPkgNames[nameRef.String()] {
			return identity{}, []validationErr{
				objectValidationErr(fieldMetadata, validationErr{
					Field: fieldName,
					Msg:   "duplicate name: " + nameRef.String(),
				}),
			}
		}
		mPkgNames[nameRef.String()] = true

		displayNameRef := p.getRefWithKnownEnvs(o.Spec, fieldName)
		identity := identity{
			name:        nameRef,
			displayName: displayNameRef,
		}
		if !resourceUniqueByName {
			return identity, nil
		}

		name := identity.Name()
		if uniqNames[name] {
			return identity, []validationErr{
				objectValidationErr(fieldSpec, validationErr{
					Field: fieldName,
					Msg:   "duplicate name: " + nameRef.String(),
				}),
			}
		}
		uniqNames[name] = true

		return identity, nil
	}
}

func (p *Template) getRefWithKnownEnvs(r Resource, field string) *references {
	nameRef := r.references(field)
	if v, ok := p.mEnvVals[nameRef.EnvRef]; ok {
		nameRef.val = v
	}
	return nameRef
}

func (p *Template) setRefs(refs ...*references) {
	for _, ref := range refs {
		if ref.Secret != "" {
			p.mSecrets[ref.Secret] = false
		}
		if ref.EnvRef != "" {
			p.mEnv[ref.EnvRef] = p.mEnvVals[ref.EnvRef] != nil
		}
	}
}

func parseAxis(ra Resource, domain []float64) *axis {
	return &axis{
		Base:   ra.stringShort(fieldAxisBase),
		Label:  ra.stringShort(fieldAxisLabel),
		Name:   ra.Name(),
		Prefix: ra.stringShort(fieldPrefix),
		Scale:  ra.stringShort(fieldAxisScale),
		Suffix: ra.stringShort(fieldSuffix),
		Domain: domain,
	}
}

func parseColor(rc Resource) *color {
	return &color{
		ID:    rc.stringShort("id"),
		Name:  rc.Name(),
		Type:  rc.stringShort(fieldType),
		Hex:   rc.stringShort(fieldColorHex),
		Value: flt64Ptr(rc.float64Short(fieldValue)),
	}
}

func (p *Template) parseChart(dashMetaName string, chartIdx int, r Resource) (*chart, []validationErr) {
	ck, err := r.chartKind()
	if err != nil {
		return nil, []validationErr{{
			Field: fieldKind,
			Msg:   err.Error(),
		}}
	}

	c := chart{
		Kind:                       ck,
		Name:                       r.Name(),
		BinSize:                    r.intShort(fieldChartBinSize),
		BinCount:                   r.intShort(fieldChartBinCount),
		Geom:                       r.stringShort(fieldChartGeom),
		Height:                     r.intShort(fieldChartHeight),
		Note:                       r.stringShort(fieldChartNote),
		NoteOnEmpty:                r.boolShort(fieldChartNoteOnEmpty),
		Position:                   r.stringShort(fieldChartPosition),
		Prefix:                     r.stringShort(fieldPrefix),
		Shade:                      r.boolShort(fieldChartShade),
		HoverDimension:             r.stringShort(fieldChartHoverDimension),
		Suffix:                     r.stringShort(fieldSuffix),
		TickPrefix:                 r.stringShort(fieldChartTickPrefix),
		TickSuffix:                 r.stringShort(fieldChartTickSuffix),
		TimeFormat:                 r.stringShort(fieldChartTimeFormat),
		Width:                      r.intShort(fieldChartWidth),
		XCol:                       r.stringShort(fieldChartXCol),
		GenerateXAxisTicks:         r.slcStr(fieldChartGenerateXAxisTicks),
		XTotalTicks:                r.intShort(fieldChartXTotalTicks),
		XTickStart:                 r.float64Short(fieldChartXTickStart),
		XTickStep:                  r.float64Short(fieldChartXTickStep),
		YCol:                       r.stringShort(fieldChartYCol),
		GenerateYAxisTicks:         r.slcStr(fieldChartGenerateYAxisTicks),
		YTotalTicks:                r.intShort(fieldChartYTotalTicks),
		YTickStart:                 r.float64Short(fieldChartYTickStart),
		YTickStep:                  r.float64Short(fieldChartYTickStep),
		XPos:                       r.intShort(fieldChartXPos),
		YPos:                       r.intShort(fieldChartYPos),
		FillColumns:                r.slcStr(fieldChartFillColumns),
		YLabelColumnSeparator:      r.stringShort(fieldChartYLabelColumnSeparator),
		YLabelColumns:              r.slcStr(fieldChartYLabelColumns),
		YSeriesColumns:             r.slcStr(fieldChartYSeriesColumns),
		UpperColumn:                r.stringShort(fieldChartUpperColumn),
		MainColumn:                 r.stringShort(fieldChartMainColumn),
		LowerColumn:                r.stringShort(fieldChartLowerColumn),
		LegendColorizeRows:         r.boolShort(fieldChartLegendColorizeRows),
		LegendHide:                 r.boolShort(fieldChartLegendHide),
		LegendOpacity:              r.float64Short(fieldChartLegendOpacity),
		LegendOrientationThreshold: r.intShort(fieldChartLegendOrientationThreshold),
		Zoom:                       r.float64Short(fieldChartGeoZoom),
		Center:                     center{Lat: r.float64Short(fieldChartGeoCenterLat), Lon: r.float64Short(fieldChartGeoCenterLon)},
		MapStyle:                   r.stringShort(fieldChartGeoMapStyle),
		AllowPanAndZoom:            r.boolShort(fieldChartGeoAllowPanAndZoom),
		DetectCoordinateFields:     r.boolShort(fieldChartGeoDetectCoordinateFields),
	}

	if presStaticLeg, ok := r[fieldChartStaticLegend].(StaticLegend); ok {
		c.StaticLegend = presStaticLeg
	} else {
		if staticLeg, ok := ifaceToResource(r[fieldChartStaticLegend]); ok {
			c.StaticLegend.ColorizeRows = staticLeg.boolShort(fieldChartStaticLegendColorizeRows)
			c.StaticLegend.HeightRatio = staticLeg.float64Short(fieldChartStaticLegendHeightRatio)
			c.StaticLegend.Hide = staticLeg.boolShort(fieldChartStaticLegendHide)
			c.StaticLegend.Opacity = staticLeg.float64Short(fieldChartStaticLegendOpacity)
			c.StaticLegend.OrientationThreshold = staticLeg.intShort(fieldChartStaticLegendOrientationThreshold)
			c.StaticLegend.ValueAxis = staticLeg.stringShort(fieldChartStaticLegendValueAxis)
			c.StaticLegend.WidthRatio = staticLeg.float64Short(fieldChartStaticLegendWidthRatio)
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
		q, vErrs := p.parseChartQueries(dashMetaName, chartIdx, r.slcResource(fieldChartQueries))
		if len(vErrs) > 0 {
			failures = append(failures, validationErr{
				Field:  "queries",
				Nested: vErrs,
			})
		}
		c.Queries = q
	}

	if presentColors, ok := r[fieldChartColors].(colors); ok {
		c.Colors = presentColors
	} else {
		for _, rc := range r.slcResource(fieldChartColors) {
			c.Colors = append(c.Colors, parseColor(rc))
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

			c.Axes = append(c.Axes, *parseAxis(ra, domain))
		}
	}

	if presentGeoLayers, ok := r[fieldChartGeoLayers].(geoLayers); ok {
		c.GeoLayers = presentGeoLayers
	} else {
		parseGeoAxis := func(r Resource, field string) *axis {
			if axis, ok := r[field].(*axis); ok {
				return axis
			} else {
				if leg, ok := ifaceToResource(r[field]); ok {
					return parseAxis(leg, nil)
				}
			}
			return nil
		}

		for _, rl := range r.slcResource(fieldChartGeoLayers) {
			gl := geoLayer{
				Type:               rl.stringShort(fieldChartGeoLayerType),
				RadiusField:        rl.stringShort(fieldChartGeoLayerRadiusField),
				ColorField:         rl.stringShort(fieldChartGeoLayerColorField),
				IntensityField:     rl.stringShort(fieldChartGeoLayerIntensityField),
				Radius:             int32(rl.intShort(fieldChartGeoLayerRadius)),
				Blur:               int32(rl.intShort(fieldChartGeoLayerBlur)),
				RadiusDimension:    parseGeoAxis(rl, fieldChartGeoLayerRadiusDimension),
				ColorDimension:     parseGeoAxis(rl, fieldChartGeoLayerColorDimension),
				IntensityDimension: parseGeoAxis(rl, fieldChartGeoLayerIntensityDimension),
				InterpolateColors:  rl.boolShort(fieldChartGeoLayerInterpolateColors),
				TrackWidth:         int32(rl.intShort(fieldChartGeoLayerTrackWidth)),
				Speed:              int32(rl.intShort(fieldChartGeoLayerSpeed)),
				RandomColors:       rl.boolShort(fieldChartGeoLayerRandomColors),
				IsClustered:        rl.boolShort(fieldChartGeoLayerIsClustered),
			}
			if presentColors, ok := rl[fieldChartGeoLayerViewColors].(colors); ok {
				gl.ViewColors = presentColors
			} else {
				for _, rc := range rl.slcResource(fieldChartGeoLayerViewColors) {
					gl.ViewColors = append(gl.ViewColors, parseColor(rc))
				}
			}
			c.GeoLayers = append(c.GeoLayers, &gl)
		}
	}

	if tableOptsRes, ok := ifaceToResource(r[fieldChartTableOptions]); ok {
		c.TableOptions = tableOptions{
			VerticalTimeAxis: tableOptsRes.boolShort(fieldChartTableOptionVerticalTimeAxis),
			SortByField:      tableOptsRes.stringShort(fieldChartTableOptionSortBy),
			Wrapping:         tableOptsRes.stringShort(fieldChartTableOptionWrapping),
			FixFirstColumn:   tableOptsRes.boolShort(fieldChartTableOptionFixFirstColumn),
		}
	}

	for _, fieldOptRes := range r.slcResource(fieldChartFieldOptions) {
		c.FieldOptions = append(c.FieldOptions, fieldOption{
			FieldName:   fieldOptRes.stringShort(fieldChartFieldOptionFieldName),
			DisplayName: fieldOptRes.stringShort(fieldChartFieldOptionDisplayName),
			Visible:     fieldOptRes.boolShort(fieldChartFieldOptionVisible),
		})
	}

	if failures = append(failures, c.validProperties()...); len(failures) > 0 {
		return nil, failures
	}

	return &c, nil
}

func (p *Template) parseChartQueries(dashMetaName string, chartIdx int, resources []Resource) (queries, []validationErr) {
	var (
		q     queries
		vErrs []validationErr
	)
	for i, rq := range resources {
		source := rq.stringShort(fieldQuery)
		if source == "" {
			continue
		}
		prefix := fmt.Sprintf("dashboards[%s].spec.charts[%d].queries[%d]", dashMetaName, chartIdx, i)
		qq, err := p.parseQuery(prefix, source, rq.slcResource(fieldParams), nil)
		if err != nil {
			vErrs = append(vErrs, validationErr{
				Field: "query",
				Index: intPtr(i),
				Msg:   err.Error(),
			})
		}
		q = append(q, qq)
	}
	return q, vErrs
}

func (p *Template) parseQuery(prefix, source string, params, task []Resource) (query, error) {
	files := parser.ParseSource(source).Files
	if len(files) != 1 {
		return query{}, influxErr(errors2.EInvalid, "invalid query source")
	}

	q := query{
		Query: strings.TrimSpace(source),
	}

	mParams := make(map[string]*references)
	tParams := make(map[string]*references)

	paramsOpt, paramsErr := edit.GetOption(files[0], "params")
	taskOpt, taskErr := edit.GetOption(files[0], "task")
	if paramsErr != nil && taskErr != nil {
		return q, nil
	}

	if paramsErr == nil {
		obj, ok := paramsOpt.(*ast.ObjectExpression)
		if ok {
			for _, p := range obj.Properties {
				sl, ok := p.Key.(*ast.Identifier)
				if !ok {
					continue
				}

				mParams[sl.Name] = &references{
					EnvRef:     sl.Name,
					defaultVal: valFromExpr(p.Value),
					valType:    p.Value.Type(),
				}
			}
		}
	}

	if taskErr == nil {
		tobj, ok := taskOpt.(*ast.ObjectExpression)
		if ok {
			for _, p := range tobj.Properties {
				sl, ok := p.Key.(*ast.Identifier)
				if !ok {
					continue
				}

				tParams[sl.Name] = &references{
					EnvRef:     sl.Name,
					defaultVal: valFromExpr(p.Value),
					valType:    p.Value.Type(),
				}
			}
		}
	}

	// override defaults here maybe?
	for _, pr := range params {
		field := pr.stringShort(fieldKey)
		if field == "" {
			continue
		}

		if _, ok := mParams[field]; !ok {
			mParams[field] = &references{EnvRef: field}
		}
		if def, ok := pr[fieldDefault]; ok {
			mParams[field].defaultVal = def
		}
		if valtype, ok := pr.string(fieldType); ok {
			mParams[field].valType = valtype
		}
	}

	var err error
	for _, pr := range task {
		field := pr.stringShort(fieldKey)
		if field == "" {
			continue
		}

		if _, ok := tParams[field]; !ok {
			tParams[field] = &references{EnvRef: field}
		}

		if valtype, ok := pr.string(fieldType); ok {
			tParams[field].valType = valtype
		}

		if def, ok := pr[fieldDefault]; ok {
			switch tParams[field].valType {
			case "duration":
				switch defDur := def.(type) {
				case string:
					tParams[field].defaultVal, err = time.ParseDuration(defDur)
					if err != nil {
						return query{}, influxErr(errors2.EInvalid, err.Error())
					}
				case time.Duration:
					tParams[field].defaultVal = defDur
				}
			default:
				tParams[field].defaultVal = def
			}
		}
	}

	for _, ref := range mParams {
		envRef := fmt.Sprintf("%s.params.%s", prefix, ref.EnvRef)
		q.params = append(q.params, &references{
			EnvRef:     envRef,
			defaultVal: ref.defaultVal,
			val:        p.mEnvVals[envRef],
			valType:    ref.valType,
		})
	}

	for _, ref := range tParams {
		envRef := fmt.Sprintf("%s.task.%s", prefix, ref.EnvRef)
		q.task = append(q.task, &references{
			EnvRef:     envRef,
			defaultVal: ref.defaultVal,
			val:        p.mEnvVals[envRef],
			valType:    ref.valType,
		})
	}
	return q, nil
}

func valFromExpr(p ast.Expression) interface{} {
	switch literal := p.(type) {
	case *ast.CallExpression:
		sl, ok := literal.Callee.(*ast.Identifier)
		if ok && sl.Name == "now" {
			return "now()"
		}
		return nil
	case *ast.DateTimeLiteral:
		return ast.DateTimeFromLiteral(literal)
	case *ast.FloatLiteral:
		return ast.FloatFromLiteral(literal)
	case *ast.IntegerLiteral:
		return ast.IntegerFromLiteral(literal)
	case *ast.DurationLiteral:
		dur, _ := ast.DurationFrom(literal, time.Time{})
		return dur
	case *ast.StringLiteral:
		return ast.StringFromLiteral(literal)
	case *ast.UnaryExpression:
		// a signed duration is represented by a UnaryExpression.
		// it is the only unary expression allowed.
		v := valFromExpr(literal.Argument)
		if dur, ok := v.(time.Duration); ok {
			switch literal.Operator {
			case ast.SubtractionOperator:
				return "-" + dur.String()
			}
		}
		return v
	default:
		return nil
	}
}

// dns1123LabelMaxLength is a label's max length in DNS (RFC 1123)
const dns1123LabelMaxLength int = 63

const dns1123LabelFmt string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"
const dns1123LabelErrMsg string = "a DNS-1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character"

var dns1123LabelRegexp = regexp.MustCompile("^" + dns1123LabelFmt + "$")

// isDNS1123Label tests for a string that conforms to the definition of a label in
// DNS (RFC 1123).
func isDNS1123Label(value string) []string {
	var errs []string
	if len(value) > dns1123LabelMaxLength {
		errs = append(errs, fmt.Sprintf("must be no more than %d characters", dns1123LabelMaxLength))
	}
	if !dns1123LabelRegexp.MatchString(value) {
		errs = append(errs, regexError(dns1123LabelErrMsg, dns1123LabelFmt, "my-name", "123-abc"))
	}
	return errs
}

// regexError returns a string explanation of a regex validation failure.
func regexError(msg string, fmt string, examples ...string) string {
	if len(examples) == 0 {
		return msg + " (regex used for validation is '" + fmt + "')"
	}
	msg += " (e.g. "
	for i := range examples {
		if i > 0 {
			msg += " or "
		}
		msg += "'" + examples[i] + "', "
	}
	msg += "regex used for validation is '" + fmt + "')"
	return msg
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

	k := Kind(resKind)
	return k, k.OK()
}

func (r Resource) chartKind() (chartKind, error) {
	ck, _ := r.kind()
	chartKind := chartKind(normStr(string(ck)))
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

func (r Resource) duration(key string) (time.Duration, bool) {
	astDur, err := options.ParseSignedDuration(r.stringShort(key))
	if err != nil {
		return time.Duration(0), false
	}

	dur, err := ast.DurationFrom(astDur, time.Time{})
	return dur, err == nil
}

func (r Resource) durationShort(key string) time.Duration {
	dur, _ := r.duration(key)
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

func (r Resource) references(key string) *references {
	v, ok := r[key]
	if !ok {
		return &references{}
	}
	return ifaceToReference(v)
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

func ifaceToReference(i interface{}) *references {
	var ref references
	for _, f := range []string{fieldReferencesSecret, fieldReferencesEnv} {
		resBody, ok := ifaceToResource(i)
		if !ok {
			continue
		}
		if keyRes, ok := ifaceToResource(resBody[f]); ok {
			switch f {
			case fieldReferencesEnv:
				ref.EnvRef = keyRes.stringShort(fieldKey)
				ref.defaultVal = keyRes[fieldDefault]
			case fieldReferencesSecret:
				ref.Secret = keyRes.stringShort(fieldKey)
			}
		}
	}
	if ref.hasValue() {
		return &ref
	}

	return &references{val: i}
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

// ParseError is the error from parsing the given package. The ParseError
// behavior provides a list of resources that failed and all validations
// that failed for that resource. A resource can multiple errors, and
// a parseErr can have multiple resources which themselves can have
// multiple validation failures.
type ParseError interface {
	ValidationErrs() []ValidationErr
}

// NewParseError creates a new parse error from existing validation errors.
func NewParseError(errs ...ValidationErr) error {
	if len(errs) == 0 {
		return nil
	}
	return &parseErr{rawErrs: errs}
}

type (
	parseErr struct {
		Resources []resourceErr
		rawErrs   []ValidationErr
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
	var (
		errMsg   []string
		seenErrs = make(map[string]bool)
	)
	for _, ve := range append(e.ValidationErrs(), e.rawErrs...) {
		msg := ve.Error()
		if seenErrs[msg] {
			continue
		}
		seenErrs[msg] = true
		errMsg = append(errMsg, ve.Error())
	}

	return strings.Join(errMsg, "\n\t")
}

func (e *parseErr) ValidationErrs() []ValidationErr {
	errs := e.rawErrs[:]
	for _, r := range e.Resources {
		rootErr := ValidationErr{
			Kind: r.Kind,
		}
		for _, v := range r.RootErrs {
			errs = append(errs, traverseErrs(rootErr, v)...)
		}

		rootErr.Indexes = []*int{r.Idx}
		rootErr.Fields = []string{"root"}
		for _, v := range append(r.ValidationErrs, r.AssociationErrs...) {
			errs = append(errs, traverseErrs(rootErr, v)...)
		}
	}

	// used to provide a means to == or != in the map lookup
	// to remove duplicate errors
	type key struct {
		kind    string
		fields  string
		indexes string
		reason  string
	}

	m := make(map[key]bool)
	var out []ValidationErr
	for _, verr := range errs {
		k := key{
			kind:   verr.Kind,
			fields: strings.Join(verr.Fields, ":"),
			reason: verr.Reason,
		}
		var indexes []string
		for _, idx := range verr.Indexes {
			if idx == nil {
				continue
			}
			indexes = append(indexes, strconv.Itoa(*idx))
		}
		k.indexes = strings.Join(indexes, ":")
		if m[k] {
			continue
		}
		m[k] = true
		out = append(out, verr)
	}

	return out
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
	if _, ok := err.(*parseErr); ok {
		return true
	}

	iErr, ok := err.(*errors2.Error)
	if !ok {
		return false
	}
	return IsParseErr(iErr.Err)
}

func objectValidationErr(field string, vErrs ...validationErr) validationErr {
	return validationErr{
		Field:  field,
		Nested: vErrs,
	}
}

func normStr(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}
