package pkger

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/notification"
	icheck "github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
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

type assocMapKey struct {
	resType influxdb.ResourceType
	name    string
}

type assocMapVal struct {
	exists bool
	v      interface{}
}

func (l assocMapVal) ID() influxdb.ID {
	if t, ok := l.v.(labelAssociater); ok {
		return t.ID()
	}
	return 0
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
	id influxdb.ID
	identity

	Color       string
	Description string
	associationMapping

	// exists provides context for a resource that already
	// exists in the platform. If a resource already exists(exists=true)
	// then the ID should be populated.
	existing *influxdb.Label
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
				ResourceID:      SafeID(v.ID()),
				ResourcePkgName: v.PkgName(),
				ResourceName:    resource.name,
				ResourceType:    resource.resType,
				LabelID:         SafeID(l.ID()),
				LabelPkgName:    l.PkgName(),
				LabelName:       l.Name(),
			})
		}
	}

	return mappings
}

func (l *label) ID() influxdb.ID {
	if l.id != 0 {
		return l.id
	}
	if l.existing != nil {
		return l.existing.ID
	}
	return 0
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
	return s[i].Name() < s[j].Name()
}

func (s sortedLabels) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
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

type notificationEndpointKind int

const (
	notificationKindHTTP notificationEndpointKind = iota + 1
	notificationKindPagerDuty
	notificationKindSlack
)

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
