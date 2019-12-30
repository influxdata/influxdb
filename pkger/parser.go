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

	mLabels                map[string]*label
	mBuckets               map[string]*bucket
	mChecks                map[string]*check
	mDashboards            []*dashboard
	mNotificationEndpoints map[string]*notificationEndpoint
	mNotificationRules     []*notificationRule
	mTasks                 []*task
	mTelegrafs             []*telegraf
	mVariables             map[string]*variable

	mSecrets map[string]bool

	isVerified bool // dry run has verified pkg resources with existing resources
	isParsed   bool // indicates the pkg has been parsed and all resources graphed accordingly
}

// Summary returns a package Summary that describes all the resources and
// associations the pkg contains. It is very useful for informing users of
// the changes that will take place when this pkg would be applied.
func (p *Pkg) Summary() Summary {
	var sum Summary

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

	sort.Slice(buckets, func(i, j int) bool { return buckets[i].name < buckets[j].name })

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
	return p.eachResource(KindBucket, 2, func(r Resource) []validationErr {
		if _, ok := p.mBuckets[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		bkt := &bucket{
			name:        r.Name(),
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
			p.mLabels[l.Name()].setMapping(bkt, false)
			return nil
		})
		sort.Sort(bkt.labels)

		p.mBuckets[r.Name()] = bkt

		return append(failures, bkt.valid()...)
	})
}

func (p *Pkg) graphLabels() *parseErr {
	p.mLabels = make(map[string]*label)
	return p.eachResource(KindLabel, 2, func(r Resource) []validationErr {
		if _, ok := p.mLabels[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}
		p.mLabels[r.Name()] = &label{
			name:        r.Name(),
			Color:       r.stringShort(fieldLabelColor),
			Description: r.stringShort(fieldDescription),
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
	for _, k := range checkKinds {
		err := p.eachResource(k.kind, 1, func(r Resource) []validationErr {
			if _, ok := p.mChecks[r.Name()]; ok {
				return []validationErr{{
					Field: "name",
					Msg:   "duplicate name: " + r.Name(),
				}}
			}

			ch := &check{
				kind:          k.checkKind,
				name:          r.Name(),
				description:   r.stringShort(fieldDescription),
				every:         r.durationShort(fieldEvery),
				level:         r.stringShort(fieldLevel),
				offset:        r.durationShort(fieldOffset),
				query:         strings.TrimSpace(r.stringShort(fieldQuery)),
				reportZero:    r.boolShort(fieldCheckReportZero),
				staleTime:     r.durationShort(fieldCheckStaleTime),
				status:        normStr(r.stringShort(fieldStatus)),
				statusMessage: r.stringShort(fieldCheckStatusMessageTemplate),
				timeSince:     r.durationShort(fieldCheckTimeSince),
			}
			for _, tagRes := range r.slcResource(fieldCheckTags) {
				ch.tags = append(ch.tags, struct{ k, v string }{
					k: tagRes.stringShort(fieldKey),
					v: tagRes.stringShort(fieldValue),
				})
			}
			for _, th := range r.slcResource(fieldCheckThresholds) {
				ch.thresholds = append(ch.thresholds, threshold{
					threshType: thresholdType(normStr(th.stringShort(fieldType))),
					allVals:    th.boolShort(fieldCheckAllValues),
					level:      strings.TrimSpace(strings.ToUpper(th.stringShort(fieldLevel))),
					max:        th.float64Short(fieldMax),
					min:        th.float64Short(fieldMin),
					val:        th.float64Short(fieldValue),
				})
			}

			failures := p.parseNestedLabels(r, func(l *label) error {
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
	return p.eachResource(KindDashboard, 2, func(r Resource) []validationErr {
		dash := &dashboard{
			name:        r.Name(),
			Description: r.stringShort(fieldDescription),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			dash.labels = append(dash.labels, l)
			p.mLabels[l.Name()].setMapping(dash, false)
			return nil
		})
		sort.Sort(dash.labels)

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
		err := p.eachResource(nk.kind, 1, func(r Resource) []validationErr {
			if _, ok := p.mNotificationEndpoints[r.Name()]; ok {
				return []validationErr{{
					Field: "name",
					Msg:   "duplicate name: " + r.Name(),
				}}
			}

			endpoint := &notificationEndpoint{
				kind:        nk.notificationKind,
				name:        r.Name(),
				description: r.stringShort(fieldDescription),
				method:      strings.TrimSpace(strings.ToUpper(r.stringShort(fieldNotificationEndpointHTTPMethod))),
				httpType:    normStr(r.stringShort(fieldType)),
				password:    r.references(fieldNotificationEndpointPassword),
				routingKey:  r.references(fieldNotificationEndpointRoutingKey),
				status:      normStr(r.stringShort(fieldStatus)),
				token:       r.references(fieldNotificationEndpointToken),
				url:         r.stringShort(fieldNotificationEndpointURL),
				username:    r.references(fieldNotificationEndpointUsername),
			}
			failures := p.parseNestedLabels(r, func(l *label) error {
				endpoint.labels = append(endpoint.labels, l)
				p.mLabels[l.Name()].setMapping(endpoint, false)
				return nil
			})
			sort.Sort(endpoint.labels)

			refs := []references{endpoint.password, endpoint.routingKey, endpoint.token, endpoint.username}
			for _, ref := range refs {
				if secret := ref.Secret; secret != "" {
					p.mSecrets[secret] = false
				}
			}

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
	return p.eachResource(KindNotificationRule, 1, func(r Resource) []validationErr {
		rule := &notificationRule{
			name:         r.Name(),
			endpointName: r.stringShort(fieldNotificationRuleEndpointName),
			description:  r.stringShort(fieldDescription),
			channel:      r.stringShort(fieldNotificationRuleChannel),
			every:        r.durationShort(fieldEvery),
			msgTemplate:  r.stringShort(fieldNotificationRuleMessageTemplate),
			offset:       r.durationShort(fieldOffset),
			status:       normStr(r.stringShort(fieldStatus)),
		}

		for _, sRule := range r.slcResource(fieldNotificationRuleStatusRules) {
			rule.statusRules = append(rule.statusRules, struct{ curLvl, prevLvl string }{
				curLvl:  strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRuleCurrentLevel))),
				prevLvl: strings.TrimSpace(strings.ToUpper(sRule.stringShort(fieldNotificationRulePreviousLevel))),
			})
		}

		for _, tRule := range r.slcResource(fieldNotificationRuleTagRules) {
			rule.tagRules = append(rule.tagRules, struct{ k, v, op string }{
				k:  tRule.stringShort(fieldKey),
				v:  tRule.stringShort(fieldValue),
				op: normStr(tRule.stringShort(fieldOperator)),
			})
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
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
	return p.eachResource(KindTask, 1, func(r Resource) []validationErr {
		t := &task{
			name:        r.Name(),
			cron:        r.stringShort(fieldTaskCron),
			description: r.stringShort(fieldDescription),
			every:       r.durationShort(fieldEvery),
			offset:      r.durationShort(fieldOffset),
			query:       strings.TrimSpace(r.stringShort(fieldQuery)),
			status:      normStr(r.stringShort(fieldStatus)),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
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
	return p.eachResource(KindTelegraf, 0, func(r Resource) []validationErr {
		tele := new(telegraf)
		tele.config.Name = r.Name()
		tele.config.Description = r.stringShort(fieldDescription)

		failures := p.parseNestedLabels(r, func(l *label) error {
			tele.labels = append(tele.labels, l)
			p.mLabels[l.Name()].setMapping(tele, false)
			return nil
		})
		sort.Sort(tele.labels)

		tele.config.Config = r.stringShort(fieldTelegrafConfig)
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
	return p.eachResource(KindVariable, 1, func(r Resource) []validationErr {
		if _, ok := p.mVariables[r.Name()]; ok {
			return []validationErr{{
				Field: "name",
				Msg:   "duplicate name: " + r.Name(),
			}}
		}

		newVar := &variable{
			name:        r.Name(),
			Description: r.stringShort(fieldDescription),
			Type:        normStr(r.stringShort(fieldType)),
			Query:       strings.TrimSpace(r.stringShort(fieldQuery)),
			Language:    normStr(r.stringShort(fieldLanguage)),
			ConstValues: r.slcStr(fieldValues),
			MapValues:   r.mapStrStr(fieldValues),
		}

		failures := p.parseNestedLabels(r, func(l *label) error {
			newVar.labels = append(newVar.labels, l)
			p.mLabels[l.Name()].setMapping(newVar, false)
			//p.mLabels[l.Name()].setVariableMapping(newVar, false)
			return nil
		})
		sort.Sort(newVar.labels)

		p.mVariables[r.Name()] = newVar

		return append(failures, newVar.valid()...)
	})
}

func (p *Pkg) eachResource(resourceKind Kind, minNameLen int, fn func(r Resource) []validationErr) *parseErr {
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

		if len(r.Name()) < minNameLen {
			pErr.append(resourceErr{
				Kind: k.String(),
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

func (r Resource) references(key string) references {
	v, ok := r[key]
	if !ok {
		return references{}
	}

	var ref references
	for _, f := range []string{fieldReferencesSecret} {
		resBody, ok := ifaceToResource(v)
		if !ok {
			continue
		}
		if keyRes, ok := ifaceToResource(resBody[f]); ok {
			ref.Secret = keyRes.stringShort(fieldKey)
		}
	}
	if ref.Secret != "" {
		return ref
	}

	return references{val: v}
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

	// these 2 maps are used to eliminate duplicates that come
	// from dependencies while keeping the Resource that has any
	// associations. If there are no associations, then the resources
	// are no different from one another.
	m := make(map[key]bool)
	res := make(map[key]Resource)

	out := make([]Resource, 0, len(resources))
	for _, r := range resources {
		k, err := r.kind()
		if err != nil {
			continue
		}
		if err := k.OK(); err != nil {
			continue
		}

		if kindsUniqByName[k] {
			rKey := key{kind: k, name: r.Name()}
			if hasAssociations, ok := m[rKey]; ok && hasAssociations {
				continue
			}
			_, hasAssociations := r[fieldAssociations]
			m[rKey] = hasAssociations
			res[rKey] = r
			continue
		}
		out = append(out, r)
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
		rootErr.Fields = []string{"spec.resources"}
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
