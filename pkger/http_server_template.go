package pkger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	pctx "github.com/influxdata/influxdb/v2/context"
	ierrors "github.com/influxdata/influxdb/v2/kit/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pkg/jsonnet"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const RoutePrefixTemplates = "/api/v2/templates"

// HTTPServerTemplates is a server that manages the templates HTTP transport.
type HTTPServerTemplates struct {
	chi.Router
	api    *kithttp.API
	logger *zap.Logger
	svc    SVC
}

// NewHTTPServerTemplates constructs a new http server.
func NewHTTPServerTemplates(log *zap.Logger, svc SVC) *HTTPServerTemplates {
	svr := &HTTPServerTemplates{
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		logger: log,
		svc:    svc,
	}

	exportAllowContentTypes := middleware.AllowContentType("text/yml", "application/x-yaml", "application/json")
	setJSONContentType := middleware.SetHeader("Content-Type", "application/json; charset=utf-8")

	r := chi.NewRouter()
	{
		r.With(exportAllowContentTypes).Post("/export", svr.export)
		r.With(setJSONContentType).Post("/apply", svr.apply)
	}

	svr.Router = r
	return svr
}

// Prefix provides the prefix to this route tree.
func (s *HTTPServerTemplates) Prefix() string {
	return RoutePrefixTemplates
}

// ReqExportOrgIDOpt provides options to export resources by organization id.
type ReqExportOrgIDOpt struct {
	OrgID   string `json:"orgID"`
	Filters struct {
		ByLabel        []string `json:"byLabel"`
		ByResourceKind []Kind   `json:"byResourceKind"`
	} `json:"resourceFilters"`
}

// ReqExport is a request body for the export endpoint.
type ReqExport struct {
	StackID   string              `json:"stackID"`
	OrgIDs    []ReqExportOrgIDOpt `json:"orgIDs"`
	Resources []ResourceToClone   `json:"resources"`
}

// OK validates a create request.
func (r *ReqExport) OK() error {
	if len(r.Resources) == 0 && len(r.OrgIDs) == 0 && r.StackID == "" {
		return &errors.Error{
			Code: errors.EUnprocessableEntity,
			Msg:  "at least 1 resource, 1 org id, or stack id must be provided",
		}
	}

	for _, org := range r.OrgIDs {
		if _, err := platform.IDFromString(org.OrgID); err != nil {
			return &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("provided org id is invalid: %q", org.OrgID),
			}
		}
	}

	if r.StackID != "" {
		_, err := platform.IDFromString(r.StackID)
		return err
	}
	return nil
}

// RespExport is a response body for the create template endpoint.
type RespExport []Object

func (s *HTTPServerTemplates) export(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqExport
	if err := s.api.DecodeJSON(r.Body, &reqBody); err != nil {
		s.api.Err(w, r, err)
		return
	}
	defer r.Body.Close()

	opts := []ExportOptFn{
		ExportWithExistingResources(reqBody.Resources...),
	}
	for _, orgIDStr := range reqBody.OrgIDs {
		orgID, err := platform.IDFromString(orgIDStr.OrgID)
		if err != nil {
			continue
		}
		opts = append(opts, ExportWithAllOrgResources(ExportByOrgIDOpt{
			OrgID:         *orgID,
			LabelNames:    orgIDStr.Filters.ByLabel,
			ResourceKinds: orgIDStr.Filters.ByResourceKind,
		}))
	}

	if reqBody.StackID != "" {
		stackID, err := platform.IDFromString(reqBody.StackID)
		if err != nil {
			s.api.Err(w, r, &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("invalid stack ID provided: %q", reqBody.StackID),
			})
			return
		}
		opts = append(opts, ExportWithStackID(*stackID))
	}

	newTemplate, err := s.svc.Export(r.Context(), opts...)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	resp := RespExport(newTemplate.Objects)
	if resp == nil {
		resp = []Object{}
	}

	var enc encoder
	switch templateEncoding(r.Header.Get("Accept")) {
	case EncodingYAML:
		enc = yaml.NewEncoder(w)
		w.Header().Set("Content-Type", "application/x-yaml")
	default:
		enc = newJSONEnc(w)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	s.encResp(w, r, enc, http.StatusOK, resp)
}

// ReqTemplateRemote provides a package via a remote (i.e. a gist). If content type is not
// provided then the service will do its best to discern the content type of the
// contents.
type ReqTemplateRemote struct {
	URL         string `json:"url" yaml:"url"`
	ContentType string `json:"contentType" yaml:"contentType"`
}

// Encoding returns the encoding type that corresponds to the given content type.
func (p ReqTemplateRemote) Encoding() Encoding {
	return convertEncoding(p.ContentType, p.URL)
}

type ReqRawTemplate struct {
	ContentType string          `json:"contentType" yaml:"contentType"`
	Sources     []string        `json:"sources" yaml:"sources"`
	Template    json.RawMessage `json:"contents" yaml:"contents"`
}

func (p ReqRawTemplate) Encoding() Encoding {
	var source string
	if len(p.Sources) > 0 {
		source = p.Sources[0]
	}
	return convertEncoding(p.ContentType, source)
}

// ReqRawAction is a raw action consumers can provide to change the behavior
// of the application of a template.
type ReqRawAction struct {
	Action     string          `json:"action"`
	Properties json.RawMessage `json:"properties"`
}

// ReqApply is the request body for a json or yaml body for the apply template endpoint.
type ReqApply struct {
	DryRun  bool                `json:"dryRun" yaml:"dryRun"`
	OrgID   string              `json:"orgID" yaml:"orgID"`
	StackID *string             `json:"stackID" yaml:"stackID"` // optional: non nil value signals stack should be used
	Remotes []ReqTemplateRemote `json:"remotes" yaml:"remotes"`

	RawTemplates []ReqRawTemplate `json:"templates" yaml:"templates"`
	RawTemplate  ReqRawTemplate   `json:"template" yaml:"template"`

	EnvRefs map[string]interface{} `json:"envRefs"`
	Secrets map[string]string      `json:"secrets"`

	RawActions []ReqRawAction `json:"actions"`
}

// Templates returns all templates associated with the request.
func (r ReqApply) Templates(encoding Encoding) (*Template, error) {
	var rawTemplates []*Template
	for _, rem := range r.Remotes {
		if rem.URL == "" {
			continue
		}
		template, err := Parse(rem.Encoding(), FromHTTPRequest(rem.URL), ValidSkipParseError())
		if err != nil {
			msg := fmt.Sprintf("template from url[%s] had an issue: %s", rem.URL, err.Error())
			return nil, influxErr(errors.EUnprocessableEntity, msg)
		}
		rawTemplates = append(rawTemplates, template)
	}

	for i, rawTmpl := range append(r.RawTemplates, r.RawTemplate) {
		if rawTmpl.Template == nil {
			continue
		}
		enc := encoding
		if sourceEncoding := rawTmpl.Encoding(); sourceEncoding != EncodingSource {
			enc = sourceEncoding
		}
		template, err := Parse(enc, FromReader(bytes.NewReader(rawTmpl.Template), rawTmpl.Sources...), ValidSkipParseError())
		if err != nil {
			sources := formatSources(rawTmpl.Sources)
			msg := fmt.Sprintf("template[%d] from source(s) %q had an issue: %s", i, sources, err.Error())
			return nil, influxErr(errors.EUnprocessableEntity, msg)
		}
		rawTemplates = append(rawTemplates, template)
	}

	return Combine(rawTemplates, ValidWithoutResources(), ValidSkipParseError())
}

type actionType string

// various ActionTypes the transport API speaks
const (
	ActionTypeSkipKind     actionType = "skipKind"
	ActionTypeSkipResource actionType = "skipResource"
)

func (r ReqApply) validActions() (struct {
	SkipKinds     []ActionSkipKind
	SkipResources []ActionSkipResource
}, error) {
	type actions struct {
		SkipKinds     []ActionSkipKind
		SkipResources []ActionSkipResource
	}

	unmarshalErrFn := func(err error, idx int, actionType string) error {
		msg := fmt.Sprintf("failed to unmarshal properties for actions[%d] %q", idx, actionType)
		return ierrors.Wrap(err, msg)
	}

	kindErrFn := func(err error, idx int, actionType string) error {
		msg := fmt.Sprintf("invalid kind for actions[%d] %q", idx, actionType)
		return ierrors.Wrap(err, msg)
	}

	var out actions
	for i, rawAct := range r.RawActions {
		switch a := rawAct.Action; actionType(a) {
		case ActionTypeSkipResource:
			var asr ActionSkipResource
			if err := json.Unmarshal(rawAct.Properties, &asr); err != nil {
				return actions{}, influxErr(errors.EInvalid, unmarshalErrFn(err, i, a))
			}
			if err := asr.Kind.OK(); err != nil {
				return actions{}, influxErr(errors.EInvalid, kindErrFn(err, i, a))
			}
			out.SkipResources = append(out.SkipResources, asr)
		case ActionTypeSkipKind:
			var ask ActionSkipKind
			if err := json.Unmarshal(rawAct.Properties, &ask); err != nil {
				return actions{}, influxErr(errors.EInvalid, unmarshalErrFn(err, i, a))
			}
			if err := ask.Kind.OK(); err != nil {
				return actions{}, influxErr(errors.EInvalid, kindErrFn(err, i, a))
			}
			out.SkipKinds = append(out.SkipKinds, ask)
		default:
			msg := fmt.Sprintf(
				"invalid action type %q provided for actions[%d] ; Must be one of [%s]",
				a, i, ActionTypeSkipResource,
			)
			return actions{}, influxErr(errors.EInvalid, msg)
		}
	}

	return out, nil
}

// RespApply is the response body for the apply template endpoint.
type RespApply struct {
	Sources []string `json:"sources" yaml:"sources"`
	StackID string   `json:"stackID" yaml:"stackID"`
	Diff    Diff     `json:"diff" yaml:"diff"`
	Summary Summary  `json:"summary" yaml:"summary"`

	Errors []ValidationErr `json:"errors,omitempty" yaml:"errors,omitempty"`
}

func (s *HTTPServerTemplates) apply(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqApply
	encoding, err := decodeWithEncoding(r, &reqBody)
	if err != nil {
		s.api.Err(w, r, newDecodeErr(encoding.String(), err))
		return
	}

	orgID, err := platform.IDFromString(reqBody.OrgID)
	if err != nil {
		s.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("invalid organization ID provided: %q", reqBody.OrgID),
		})
		return
	}

	var stackID platform.ID
	if reqBody.StackID != nil {
		if err := stackID.DecodeFromString(*reqBody.StackID); err != nil {
			s.api.Err(w, r, &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("invalid stack ID provided: %q", *reqBody.StackID),
			})
			return
		}
	}

	parsedTemplate, err := reqBody.Templates(encoding)
	if err != nil {
		s.api.Err(w, r, &errors.Error{
			Code: errors.EUnprocessableEntity,
			Err:  err,
		})
		return
	}

	actions, err := reqBody.validActions()
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	applyOpts := []ApplyOptFn{
		ApplyWithEnvRefs(reqBody.EnvRefs),
		ApplyWithTemplate(parsedTemplate),
		ApplyWithStackID(stackID),
	}
	for _, a := range actions.SkipResources {
		applyOpts = append(applyOpts, ApplyWithResourceSkip(a))
	}
	for _, a := range actions.SkipKinds {
		applyOpts = append(applyOpts, ApplyWithKindSkip(a))
	}

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.api.Err(w, r, err)
		return
	}
	userID := auth.GetUserID()

	if reqBody.DryRun {
		impact, err := s.svc.DryRun(r.Context(), *orgID, userID, applyOpts...)
		if IsParseErr(err) {
			s.api.Respond(w, r, http.StatusUnprocessableEntity, impactToRespApply(impact, err))
			return
		}
		if err != nil {
			s.api.Err(w, r, err)
			return
		}

		s.api.Respond(w, r, http.StatusOK, impactToRespApply(impact, nil))
		return
	}

	applyOpts = append(applyOpts, ApplyWithSecrets(reqBody.Secrets))

	impact, err := s.svc.Apply(r.Context(), *orgID, userID, applyOpts...)
	if err != nil && !IsParseErr(err) {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusCreated, impactToRespApply(impact, err))
}

func (s *HTTPServerTemplates) encResp(w http.ResponseWriter, r *http.Request, enc encoder, code int, res interface{}) {
	w.WriteHeader(code)
	if err := enc.Encode(res); err != nil {
		s.api.Err(w, r, &errors.Error{
			Msg:  fmt.Sprintf("unable to marshal; Err: %v", err),
			Code: errors.EInternal,
			Err:  err,
		})
	}
}

func impactToRespApply(impact ImpactSummary, err error) RespApply {
	out := RespApply{
		Sources: append([]string{}, impact.Sources...), // guarantee non nil slice
		StackID: impact.StackID.String(),
		Diff:    impact.Diff,
		Summary: impact.Summary,
	}
	if err != nil {
		out.Errors = convertParseErr(err)
	}
	if out.Diff.Buckets == nil {
		out.Diff.Buckets = []DiffBucket{}
	}
	if out.Diff.Checks == nil {
		out.Diff.Checks = []DiffCheck{}
	}
	if out.Diff.Dashboards == nil {
		out.Diff.Dashboards = []DiffDashboard{}
	}
	if out.Diff.Labels == nil {
		out.Diff.Labels = []DiffLabel{}
	}
	if out.Diff.LabelMappings == nil {
		out.Diff.LabelMappings = []DiffLabelMapping{}
	}
	if out.Diff.NotificationEndpoints == nil {
		out.Diff.NotificationEndpoints = []DiffNotificationEndpoint{}
	}
	if out.Diff.NotificationRules == nil {
		out.Diff.NotificationRules = []DiffNotificationRule{}
	}
	if out.Diff.NotificationRules == nil {
		out.Diff.NotificationRules = []DiffNotificationRule{}
	}
	if out.Diff.Tasks == nil {
		out.Diff.Tasks = []DiffTask{}
	}
	if out.Diff.Telegrafs == nil {
		out.Diff.Telegrafs = []DiffTelegraf{}
	}
	if out.Diff.Variables == nil {
		out.Diff.Variables = []DiffVariable{}
	}

	if out.Summary.Buckets == nil {
		out.Summary.Buckets = []SummaryBucket{}
	}
	if out.Summary.Checks == nil {
		out.Summary.Checks = []SummaryCheck{}
	}
	if out.Summary.Dashboards == nil {
		out.Summary.Dashboards = []SummaryDashboard{}
	}
	if out.Summary.Labels == nil {
		out.Summary.Labels = []SummaryLabel{}
	}
	if out.Summary.LabelMappings == nil {
		out.Summary.LabelMappings = []SummaryLabelMapping{}
	}
	if out.Summary.NotificationEndpoints == nil {
		out.Summary.NotificationEndpoints = []SummaryNotificationEndpoint{}
	}
	if out.Summary.NotificationRules == nil {
		out.Summary.NotificationRules = []SummaryNotificationRule{}
	}
	if out.Summary.NotificationRules == nil {
		out.Summary.NotificationRules = []SummaryNotificationRule{}
	}
	if out.Summary.Tasks == nil {
		out.Summary.Tasks = []SummaryTask{}
	}
	if out.Summary.TelegrafConfigs == nil {
		out.Summary.TelegrafConfigs = []SummaryTelegraf{}
	}
	if out.Summary.Variables == nil {
		out.Summary.Variables = []SummaryVariable{}
	}

	return out
}

func formatSources(sources []string) string {
	return strings.Join(sources, "; ")
}

func decodeWithEncoding(r *http.Request, v interface{}) (Encoding, error) {
	encoding := templateEncoding(r.Header.Get("Content-Type"))

	var dec interface{ Decode(interface{}) error }
	switch encoding {
	case EncodingJsonnet:
		dec = jsonnet.NewDecoder(r.Body)
	case EncodingYAML:
		dec = yaml.NewDecoder(r.Body)
	default:
		dec = json.NewDecoder(r.Body)
	}

	return encoding, dec.Decode(v)
}

func templateEncoding(contentType string) Encoding {
	switch contentType {
	case "application/x-jsonnet":
		return EncodingJsonnet
	case "text/yml", "application/x-yaml":
		return EncodingYAML
	default:
		return EncodingJSON
	}
}

func convertEncoding(ct, rawURL string) Encoding {
	ct = strings.ToLower(ct)
	urlBase := path.Ext(rawURL)
	switch {
	case ct == "jsonnet" || urlBase == ".jsonnet":
		return EncodingJsonnet
	case ct == "json" || urlBase == ".json":
		return EncodingJSON
	case ct == "yml" || ct == "yaml" || urlBase == ".yml" || urlBase == ".yaml":
		return EncodingYAML
	default:
		return EncodingSource
	}
}

type encoder interface {
	Encode(interface{}) error
}

func newJSONEnc(w io.Writer) encoder {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc
}

func convertParseErr(err error) []ValidationErr {
	pErr, ok := err.(ParseError)
	if !ok {
		return nil
	}
	return pErr.ValidationErrs()
}

func newDecodeErr(encoding string, err error) *errors.Error {
	return &errors.Error{
		Msg:  fmt.Sprintf("unable to unmarshal %s", encoding),
		Code: errors.EInvalid,
		Err:  err,
	}
}
