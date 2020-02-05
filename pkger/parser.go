package pkger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/pkg/jsonnet"
	"gopkg.in/yaml.v3"
)

type (
	// ReaderFn is used for functional inputs to abstract the individual
	// entrypoints for the reader itself.
	ReaderFn func() (io.Reader, error)

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
// we can parse both a YAML, JSON, and Jsonnet formats of the Pkg model.
func Parse(encoding Encoding, readerFn ReaderFn, opts ...ValidateOptFn) (*Pkg, error) {
	r, err := readerFn()
	if err != nil {
		return nil, err
	}

	switch encoding {
	case EncodingJSON:
		return parseJSON(r, opts...)
	case EncodingJsonnet:
		return parseJsonnet(r, opts...)
	case EncodingSource:
		return parseSource(r, opts...)
	case EncodingYAML:
		return parseYAML(r, opts...)
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

// FromHTTPRequest parses a pkg from the request body of a HTTP request. This is
// very useful when using packages that are hosted..
func FromHTTPRequest(addr string) ReaderFn {
	return func() (io.Reader, error) {
		client := http.Client{Timeout: 5 * time.Minute}
		resp, err := client.Get(addr)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return nil, err
		}
		return &buf, nil
	}
}

func parseJSON(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
	return parse(json.NewDecoder(r), opts...)
}

func parseJsonnet(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
	return parse(jsonnet.NewDecoder(r), opts...)
}

func parseSource(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
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

func parseYAML(r io.Reader, opts ...ValidateOptFn) (*Pkg, error) {
	dec := yaml.NewDecoder(r)

	var pkg Pkg
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

func parse(dec decoder, opts ...ValidateOptFn) (*Pkg, error) {
	var pkg Pkg
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
	Type       Kind     `json:"kind" yaml:"kind"`
	Metadata   Resource `json:"metadata" yaml:"metadata"`
	Spec       Resource `json:"spec" yaml:"spec"`
}

// Name returns the name of the kind.
func (k Object) Name() string {
	return k.Metadata.references("name").String()
}

// Pkg is the model for a package. The resources are more generic that one might
// expect at first glance. This was done on purpose. The way json/yaml/toml or
// w/e scripting you want to use, can have very different ways of parsing. The
// different parsers are limited for the parsers that do not come from the std
// lib (looking at you yaml/v2). This allows us to parse it and leave the matching
// to another power, the graphing of the package is handled within itself.
type Pkg struct {
	Objects []Object `json:"-" yaml:"-"`

	mLabels                map[string]*label
	mBuckets               map[string]*bucket
	mChecks                map[string]*check
	mDashboards            []*dashboard
	mNotificationEndpoints map[string]*notificationEndpoint
	mNotificationRules     []*notificationRule
	mTasks                 []*task
	mTelegrafs             []*telegraf
	mVariables             map[string]*variable

	mEnv     map[string][]*references
	mSecrets map[string]bool

	isVerified bool // dry run has verified pkg resources with existing resources
	isParsed   bool // indicates the pkg has been parsed and all resources graphed accordingly
}

// Encode is a helper for encoding the pkg correctly.
func (p *Pkg) Encode(encoding Encoding) ([]byte, error) {
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

// Summary returns a package Summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Pkg) Summary() Summary {
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
		MissingSecrets:        []string{},
		Tasks:                 []SummaryTask{},
		TelegrafConfigs:       []SummaryTelegraf{},
		Variables:             []SummaryVariable{},
	}

	// only add this after dry run has been completed
	if p.isVerified {
		sum.MissingSecrets = p.missingSecrets()
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

func (p *Pkg) applyEnvRefs(envRefs map[string]string) {
	for k, v := range envRefs {
		for _, ref := range p.mEnv[k] {
			ref.val = v
		}
	}
}

func (p *Pkg) applySecrets(secrets map[string]string) {
	for k := range secrets {
		p.mSecrets[k] = true
	}
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
func (p *Pkg) Validate(opts ...ValidateOptFn) error {
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

func (p *Pkg) buckets() []*bucket {
	buckets := make([]*bucket, 0, len(p.mBuckets))
	for _, b := range p.mBuckets {
		buckets = append(buckets, b)
	}

	sort.Slice(buckets, func(i, j int) bool { return buckets[i].name.String() < buckets[j].name.String() })

	return buckets
}

func (p *Pkg) checks() []*check {
	checks := make([]*check, 0, len(p.mChecks))
	for _, c := range p.mChecks {
		checks = append(checks, c)
	}

	sort.Slice(checks, func(i, j int) bool { return checks[i].Name() < checks[j].Name() })

	return checks
}

func (p *Pkg) labels() []*label {
	labels := make(sortedLabels, 0, len(p.mLabels))
	for _, b := range p.mLabels {
		labels = append(labels, b)
	}

	sort.Sort(labels)

	return labels
}

func (p *Pkg) dashboards() []*dashboard {
	dashes := p.mDashboards[:]
	sort.Slice(dashes, func(i, j int) bool { return dashes[i].name < dashes[j].name })
	return dashes
}

func (p *Pkg) notificationEndpoints() []*notificationEndpoint {
	endpoints := make([]*notificationEndpoint, 0, len(p.mNotificationEndpoints))
	for _, e := range p.mNotificationEndpoints {
		endpoints = append(endpoints, e)
	}
	sort.Slice(endpoints, func(i, j int) bool {
		ei, ej := endpoints[i], endpoints[j]
		if ei.kind == ej.kind {
			return ei.Name() < ej.Name()
		}
		return ei.kind < ej.kind
	})
	return endpoints
}

func (p *Pkg) notificationRules() []*notificationRule {
	rules := p.mNotificationRules[:]
	sort.Slice(rules, func(i, j int) bool { return rules[i].name < rules[j].name })
	return rules
}

func (p *Pkg) missingEnvRefs() []string {
	envRefs := make([]string, 0)
	for envRef, refs := range p.mEnv {
		for _, ref := range refs {
			if ref.val != nil {
				continue
			}
			envRefs = append(envRefs, envRef)
			break
		}
	}
	return envRefs
}

func (p *Pkg) missingSecrets() []string {
	secrets := make([]string, 0, len(p.mSecrets))
	for secret, foundInPlatform := range p.mSecrets {
		if foundInPlatform {
			continue
		}
		secrets = append(secrets, secret)
	}
	return secrets
}

func (p *Pkg) tasks() []*task {
	tasks := p.mTasks[:]

	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name() < tasks[j].Name() })

	return tasks
}

func (p *Pkg) telegrafs() []*telegraf {
	teles := p.mTelegrafs[:]
	sort.Slice(teles, func(i, j int) bool { return teles[i].Name() < teles[j].Name() })
	return teles
}

func (p *Pkg) variables() []*variable {
	vars := make([]*variable, 0, len(p.mVariables))
	for _, v := range p.mVariables {
		vars = append(vars, v)
	}

	sort.Slice(vars, func(i, j int) bool { return vars[i].name < vars[j].name })

	return vars
}

// labelMappings returns the mappings that will be created for
// valid pairs of labels and resources of which all have IDs.
// If a resource does not exist yet, a label mapping will not
// be returned for it.
func (p *Pkg) labelMappings() []SummaryLabelMapping {
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

func (p *Pkg) validResources() error {
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

func (p *Pkg) graphResources() error {
	if p.mEnv == nil {
		p.mEnv = make(map[string][]*references)
	}
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

func (p *Pkg) graphBuckets() *parseErr {
	p.mBuckets = make(map[string]*bucket)
	return p.eachResource(KindBucket, 2, func(k Object) []validationErr {
		nameRef := k.Metadata.references("name")
		if _, ok := p.mBuckets[nameRef.String()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + nameRef.String(),
			}}
		}

		bkt := &bucket{
			name:        nameRef,
			Description: k.Spec.stringShort(fieldDescription),
		}
		if rules, ok := k.Spec[fieldBucketRetentionRules].(retentionRules); ok {
			bkt.RetentionRules = rules
		} else {
			for _, r := range k.Spec.slcResource(fieldBucketRetentionRules) {
				bkt.RetentionRules = append(bkt.RetentionRules, retentionRule{
					Type:    r.stringShort(fieldType),
					Seconds: r.intShort(fieldRetentionRulesEverySeconds),
				})
			}
		}
		p.setRefs(bkt.name)

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			bkt.labels = append(bkt.labels, l)
			p.mLabels[l.Name()].setMapping(bkt, false)
			return nil
		})
		sort.Sort(bkt.labels)

		p.mBuckets[k.Name()] = bkt

		return append(failures, bkt.valid()...)
	})
}

func (p *Pkg) graphLabels() *parseErr {
	p.mLabels = make(map[string]*label)
	return p.eachResource(KindLabel, 2, func(k Object) []validationErr {
		if _, ok := p.mLabels[k.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + k.Name(),
			}}
		}
		p.mLabels[k.Name()] = &label{
			name:        k.Name(),
			Color:       k.Spec.stringShort(fieldLabelColor),
			Description: k.Spec.stringShort(fieldDescription),
		}

		return nil
	})
}

func (p *Pkg) graphChecks() *parseErr {
	p.mChecks = make(map[string]*check)

	checkKinds := []struct {
		kind      Kind
		checkKind checkKind
	}{
		{kind: KindCheckThreshold, checkKind: checkKindThreshold},
		{kind: KindCheckDeadman, checkKind: checkKindDeadman},
	}
	var pErr parseErr
	for _, checkKind := range checkKinds {
		err := p.eachResource(checkKind.kind, 1, func(k Object) []validationErr {
			if _, ok := p.mChecks[k.Name()]; ok {
				return []validationErr{{
					Field: "name",
					Msg:   "duplicate name: " + k.Name(),
				}}
			}

			ch := &check{
				kind:          checkKind.checkKind,
				name:          k.Name(),
				description:   k.Spec.stringShort(fieldDescription),
				every:         k.Spec.durationShort(fieldEvery),
				level:         k.Spec.stringShort(fieldLevel),
				offset:        k.Spec.durationShort(fieldOffset),
				query:         strings.TrimSpace(k.Spec.stringShort(fieldQuery)),
				reportZero:    k.Spec.boolShort(fieldCheckReportZero),
				staleTime:     k.Spec.durationShort(fieldCheckStaleTime),
				status:        normStr(k.Spec.stringShort(fieldStatus)),
				statusMessage: k.Spec.stringShort(fieldCheckStatusMessageTemplate),
				timeSince:     k.Spec.durationShort(fieldCheckTimeSince),
			}
			for _, tagRes := range k.Spec.slcResource(fieldCheckTags) {
				ch.tags = append(ch.tags, struct{ k, v string }{
					k: tagRes.stringShort(fieldKey),
					v: tagRes.stringShort(fieldValue),
				})
			}
			for _, th := range k.Spec.slcResource(fieldCheckThresholds) {
				ch.thresholds = append(ch.thresholds, threshold{
					threshType: thresholdType(normStr(th.stringShort(fieldType))),
					allVals:    th.boolShort(fieldCheckAllValues),
					level:      strings.TrimSpace(strings.ToUpper(th.stringShort(fieldLevel))),
					max:        th.float64Short(fieldMax),
					min:        th.float64Short(fieldMin),
					val:        th.float64Short(fieldValue),
				})
			}

			failures := p.parseNestedLabels(k.Spec, func(l *label) error {
				ch.labels = append(ch.labels, l)
				p.mLabels[l.Name()].setMapping(ch, false)
				return nil
			})
			sort.Sort(ch.labels)

			p.mChecks[ch.Name()] = ch
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

func (p *Pkg) graphDashboards() *parseErr {
	p.mDashboards = make([]*dashboard, 0)
	return p.eachResource(KindDashboard, 2, func(k Object) []validationErr {
		dash := &dashboard{
			name:        k.Name(),
			Description: k.Spec.stringShort(fieldDescription),
		}

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			dash.labels = append(dash.labels, l)
			p.mLabels[l.Name()].setMapping(dash, false)
			return nil
		})
		sort.Sort(dash.labels)

		for i, cr := range k.Spec.slcResource(fieldDashCharts) {
			ch, fails := parseChart(cr)
			if fails != nil {
				failures = append(failures, validationErr{
					Field:  fieldDashCharts,
					Index:  intPtr(i),
					Nested: fails,
				})
				continue
			}
			dash.Charts = append(dash.Charts, ch)
		}

		p.mDashboards = append(p.mDashboards, dash)

		return failures
	})
}

func (p *Pkg) graphNotificationEndpoints() *parseErr {
	p.mNotificationEndpoints = make(map[string]*notificationEndpoint)

	notificationKinds := []struct {
		kind             Kind
		notificationKind notificationKind
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
		err := p.eachResource(nk.kind, 1, func(k Object) []validationErr {
			if _, ok := p.mNotificationEndpoints[k.Name()]; ok {
				return []validationErr{{
					Field: "name",
					Msg:   "duplicate name: " + k.Name(),
				}}
			}

			endpoint := &notificationEndpoint{
				kind:        nk.notificationKind,
				name:        k.Name(),
				description: k.Spec.stringShort(fieldDescription),
				method:      strings.TrimSpace(strings.ToUpper(k.Spec.stringShort(fieldNotificationEndpointHTTPMethod))),
				httpType:    normStr(k.Spec.stringShort(fieldType)),
				password:    k.Spec.references(fieldNotificationEndpointPassword),
				routingKey:  k.Spec.references(fieldNotificationEndpointRoutingKey),
				status:      normStr(k.Spec.stringShort(fieldStatus)),
				token:       k.Spec.references(fieldNotificationEndpointToken),
				url:         k.Spec.stringShort(fieldNotificationEndpointURL),
				username:    k.Spec.references(fieldNotificationEndpointUsername),
			}
			failures := p.parseNestedLabels(k.Spec, func(l *label) error {
				endpoint.labels = append(endpoint.labels, l)
				p.mLabels[l.Name()].setMapping(endpoint, false)
				return nil
			})
			sort.Sort(endpoint.labels)

			p.setRefs(endpoint.password, endpoint.routingKey, endpoint.token, endpoint.username)

			p.mNotificationEndpoints[endpoint.Name()] = endpoint
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

func (p *Pkg) graphNotificationRules() *parseErr {
	p.mNotificationRules = make([]*notificationRule, 0)
	return p.eachResource(KindNotificationRule, 1, func(k Object) []validationErr {
		rule := &notificationRule{
			name:         k.Name(),
			endpointName: k.Spec.stringShort(fieldNotificationRuleEndpointName),
			description:  k.Spec.stringShort(fieldDescription),
			channel:      k.Spec.stringShort(fieldNotificationRuleChannel),
			every:        k.Spec.durationShort(fieldEvery),
			msgTemplate:  k.Spec.stringShort(fieldNotificationRuleMessageTemplate),
			offset:       k.Spec.durationShort(fieldOffset),
			status:       normStr(k.Spec.stringShort(fieldStatus)),
		}

		for _, sRule := range k.Spec.slcResource(fieldNotificationRuleStatusRules) {
			rule.statusRules = append(rule.statusRules, struct{ curLvl, prevLvl string }{
				curLvl:  strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRuleCurrentLevel))),
				prevLvl: strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRulePreviousLevel))),
			})
		}

		for _, tRule := range k.Spec.slcResource(fieldNotificationRuleTagRules) {
			rule.tagRules = append(rule.tagRules, struct{ k, v, op string }{
				k:  tRule.stringShort(fieldKey),
				v:  tRule.stringShort(fieldValue),
				op: normStr(tRule.stringShort(fieldOperator)),
			})
		}

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			rule.labels = append(rule.labels, l)
			p.mLabels[l.Name()].setMapping(rule, false)
			return nil
		})
		sort.Sort(rule.labels)

		p.mNotificationRules = append(p.mNotificationRules, rule)
		return append(failures, rule.valid()...)
	})
}

func (p *Pkg) graphTasks() *parseErr {
	p.mTasks = make([]*task, 0)
	return p.eachResource(KindTask, 1, func(k Object) []validationErr {
		t := &task{
			name:        k.Name(),
			cron:        k.Spec.stringShort(fieldTaskCron),
			description: k.Spec.stringShort(fieldDescription),
			every:       k.Spec.durationShort(fieldEvery),
			offset:      k.Spec.durationShort(fieldOffset),
			query:       strings.TrimSpace(k.Spec.stringShort(fieldQuery)),
			status:      normStr(k.Spec.stringShort(fieldStatus)),
		}

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			t.labels = append(t.labels, l)
			p.mLabels[l.Name()].setMapping(t, false)
			return nil
		})
		sort.Sort(t.labels)

		p.mTasks = append(p.mTasks, t)
		return append(failures, t.valid()...)
	})
}

func (p *Pkg) graphTelegrafs() *parseErr {
	p.mTelegrafs = make([]*telegraf, 0)
	return p.eachResource(KindTelegraf, 0, func(k Object) []validationErr {
		tele := new(telegraf)
		tele.config.Name = k.Name()
		tele.config.Description = k.Spec.stringShort(fieldDescription)

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			tele.labels = append(tele.labels, l)
			p.mLabels[l.Name()].setMapping(tele, false)
			return nil
		})
		sort.Sort(tele.labels)

		tele.config.Config = k.Spec.stringShort(fieldTelegrafConfig)
		if tele.config.Config == "" {
			failures = append(failures, validationErr{
				Field: fieldTelegrafConfig,
				Msg:   "no config provided",
			})
		}

		p.mTelegrafs = append(p.mTelegrafs, tele)

		return failures
	})
}

func (p *Pkg) graphVariables() *parseErr {
	p.mVariables = make(map[string]*variable)
	return p.eachResource(KindVariable, 1, func(k Object) []validationErr {
		if _, ok := p.mVariables[k.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + k.Name(),
			}}
		}

		newVar := &variable{
			name:        k.Name(),
			Description: k.Spec.stringShort(fieldDescription),
			Type:        normStr(k.Spec.stringShort(fieldType)),
			Query:       strings.TrimSpace(k.Spec.stringShort(fieldQuery)),
			Language:    normStr(k.Spec.stringShort(fieldLanguage)),
			ConstValues: k.Spec.slcStr(fieldValues),
			MapValues:   k.Spec.mapStrStr(fieldValues),
		}

		failures := p.parseNestedLabels(k.Spec, func(l *label) error {
			newVar.labels = append(newVar.labels, l)
			p.mLabels[l.Name()].setMapping(newVar, false)
			return nil
		})
		sort.Sort(newVar.labels)

		p.mVariables[k.Name()] = newVar

		return append(failures, newVar.valid()...)
	})
}

func (p *Pkg) eachResource(resourceKind Kind, minNameLen int, fn func(r Object) []validationErr) *parseErr {
	var pErr parseErr
	for i, k := range p.Objects {
		if err := k.Type.OK(); err != nil {
			pErr.append(resourceErr{
				Kind: k.Type.String(),
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
		if !k.Type.is(resourceKind) {
			continue
		}

		if k.APIVersion != APIVersion {
			pErr.append(resourceErr{
				Kind: k.Type.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					{
						Field: fieldAPIVersion,
						Msg:   fmt.Sprintf("invalid API version provided %q; must be 1 in [%s]", k.APIVersion, APIVersion),
					},
				},
			})
			continue
		}

		if len(k.Name()) < minNameLen {
			pErr.append(resourceErr{
				Kind: k.Type.String(),
				Idx:  intPtr(i),
				ValidationErrs: []validationErr{
					{
						Field: "name",
						Msg:   fmt.Sprintf("must be a string of at least %d chars in length", minNameLen),
					},
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

func (p *Pkg) parseNestedLabels(r Resource, fn func(lb *label) error) []validationErr {
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

func (p *Pkg) parseNestedLabel(nr Resource, fn func(lb *label) error) *validationErr {
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

	lb, found := p.mLabels[nr.Name()]
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

func (p *Pkg) setRefs(refs ...*references) {
	for _, ref := range refs {
		if ref.Secret != "" {
			p.mSecrets[ref.Secret] = false
		}
		if ref.EnvRef != "" {
			p.mEnv[ref.EnvRef] = append(p.mEnv[ref.EnvRef], ref)
		}
	}
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
		TickPrefix:  r.stringShort(fieldChartTickPrefix),
		Suffix:      r.stringShort(fieldSuffix),
		TickSuffix:  r.stringShort(fieldChartTickSuffix),
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
	dur, err := time.ParseDuration(r.stringShort(key))
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

	var ref references
	for _, f := range []string{fieldReferencesSecret, fieldReferencesEnv} {
		resBody, ok := ifaceToResource(v)
		if !ok {
			continue
		}
		if keyRes, ok := ifaceToResource(resBody[f]); ok {
			switch f {
			case fieldReferencesEnv:
				ref.EnvRef = keyRes.stringShort(fieldKey)
			case fieldReferencesSecret:
				ref.Secret = keyRes.stringShort(fieldKey)
			}
		}
	}
	if ref.hasValue() {
		return &ref
	}

	return &references{val: v}
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

func uniqResources(kinds []Object) []Object {
	type key struct {
		kind Kind
		name string
	}

	// these 2 maps are used to eliminate duplicates that come
	// from dependencies while keeping the Object that has any
	// associations. If there are no associations, then the kinds
	// are no different from one another.
	m := make(map[key]bool)
	res := make(map[key]Object)

	out := make([]Object, 0, len(kinds))
	for _, k := range kinds {
		if err := k.Type.OK(); err != nil {
			continue
		}

		if kindsUniqByName[k.Type] {
			rKey := key{kind: k.Type, name: k.Name()}
			if hasAssociations, ok := m[rKey]; ok && hasAssociations {
				continue
			}
			_, hasAssociations := k.Spec[fieldAssociations]
			m[rKey] = hasAssociations
			res[rKey] = k
			continue
		}
		out = append(out, k)
	}

	for _, r := range res {
		out = append(out, r)
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
	var errMsg []string
	for _, ve := range append(e.ValidationErrs(), e.rawErrs...) {
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

	iErr, ok := err.(*influxdb.Error)
	if !ok {
		return false
	}
	return IsParseErr(iErr.Err)
}

func normStr(s string) string {
	return strings.TrimSpace(strings.ToLower(s))
}
