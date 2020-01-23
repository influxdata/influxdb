package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/pkg/httpc"
	"github.com/influxdata/influxdb/pkg/jsonnet"
	"github.com/influxdata/influxdb/pkger"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const prefixPackages = "/api/v2/packages"

// HandlerPkg is a server that manages the packages HTTP transport.
type HandlerPkg struct {
	chi.Router
	influxdb.HTTPErrorHandler
	logger *zap.Logger
	svc    pkger.SVC
}

// NewHandlerPkg constructs a new http server.
func NewHandlerPkg(log *zap.Logger, errHandler influxdb.HTTPErrorHandler, svc pkger.SVC) *HandlerPkg {
	svr := &HandlerPkg{
		HTTPErrorHandler: errHandler,
		logger:           log,
		svc:              svc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	{
		r.With(middleware.AllowContentType("text/yml", "application/x-yaml", "application/json")).
			Post("/", svr.createPkg)
		r.With(middleware.SetHeader("Content-Type", "application/json; charset=utf-8")).
			Post("/apply", svr.applyPkg)
	}

	svr.Router = r
	return svr
}

// Prefix provides the prefix to this route tree.
func (s *HandlerPkg) Prefix() string {
	return prefixPackages
}

type (
	// ReqCreatePkg is a request body for the create pkg endpoint.
	ReqCreatePkg struct {
		OrgIDs    []string                `json:"orgIDs"`
		Resources []pkger.ResourceToClone `json:"resources"`
	}

	// RespCreatePkg is a response body for the create pkg endpoint.
	RespCreatePkg []pkger.Object
)

func (s *HandlerPkg) createPkg(w http.ResponseWriter, r *http.Request) {
	encoding := pkgEncoding(r.Header)

	var reqBody ReqCreatePkg
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		s.HandleHTTPError(r.Context(), newDecodeErr("json", err), w)
		return
	}
	defer r.Body.Close()

	if len(reqBody.Resources) == 0 && len(reqBody.OrgIDs) == 0 {
		s.HandleHTTPError(r.Context(), &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "at least 1 resource or 1 org id must be provided",
		}, w)
		return
	}

	opts := []pkger.CreatePkgSetFn{
		pkger.CreateWithExistingResources(reqBody.Resources...),
	}
	for _, orgIDStr := range reqBody.OrgIDs {
		orgID, err := influxdb.IDFromString(orgIDStr)
		if err != nil {
			continue
		}
		opts = append(opts, pkger.CreateWithAllOrgResources(*orgID))
	}

	newPkg, err := s.svc.CreatePkg(r.Context(), opts...)
	if err != nil {
		s.logger.Error("failed to create pkg", zap.Error(err))
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	resp := RespCreatePkg(newPkg.Objects)
	if resp == nil {
		resp = []pkger.Object{}
	}

	var enc encoder
	switch encoding {
	case pkger.EncodingYAML:
		enc = yaml.NewEncoder(w)
		w.Header().Set("Content-Type", "application/x-yaml")
	default:
		enc = newJSONEnc(w)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	s.encResp(r.Context(), w, enc, http.StatusOK, resp)
}

// PkgRemote provides a package via a remote (i.e. a gist). If content type is not
// provided then the service will do its best to discern the content type of the
// contents.
type PkgRemote struct {
	URL         string `json:"url"`
	ContentType string `json:"contentType"`
}

// Encoding returns the encoding type that corresponds to the given content type.
func (p PkgRemote) Encoding() pkger.Encoding {
	ct := strings.ToLower(p.ContentType)
	urlBase := path.Ext(p.URL)
	switch {
	case ct == "jsonnet" || urlBase == ".jsonnet":
		return pkger.EncodingJsonnet
	case ct == "json" || urlBase == ".json":
		return pkger.EncodingJSON
	case ct == "yml" || ct == "yaml" || urlBase == ".yml" || urlBase == ".yaml":
		return pkger.EncodingYAML
	default:
		return pkger.EncodingSource
	}
}

// ReqApplyPkg is the request body for a json or yaml body for the apply pkg endpoint.
type ReqApplyPkg struct {
	DryRun  bool              `json:"dryRun" yaml:"dryRun"`
	OrgID   string            `json:"orgID" yaml:"orgID"`
	Remote  PkgRemote         `json:"remote" yaml:"remote"`
	RawPkg  json.RawMessage   `json:"package" yaml:"package"`
	Secrets map[string]string `json:"secrets"`
}

// Pkg returns a pkg parsed and validated from the RawPkg field.
func (r ReqApplyPkg) Pkg(encoding pkger.Encoding) (*pkger.Pkg, error) {
	if r.Remote.URL != "" {
		return pkger.Parse(r.Remote.Encoding(), pkger.FromHTTPRequest(r.Remote.URL))
	}

	return pkger.Parse(encoding, pkger.FromReader(bytes.NewReader(r.RawPkg)))
}

// RespApplyPkg is the response body for the apply pkg endpoint.
type RespApplyPkg struct {
	Diff    pkger.Diff    `json:"diff" yaml:"diff"`
	Summary pkger.Summary `json:"summary" yaml:"summary"`

	Errors []pkger.ValidationErr `json:"errors,omitempty" yaml:"errors,omitempty"`
}

func (s *HandlerPkg) applyPkg(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqApplyPkg
	encoding, err := decodeWithEncoding(r, &reqBody)
	if err != nil {
		s.HandleHTTPError(r.Context(), newDecodeErr(encoding.String(), err), w)
		return
	}

	orgID, err := influxdb.IDFromString(reqBody.OrgID)
	if err != nil {
		s.HandleHTTPError(r.Context(), &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  fmt.Sprintf("invalid organization ID provided: %q", reqBody.OrgID),
		}, w)
		return
	}

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}
	userID := auth.GetUserID()

	parsedPkg, err := reqBody.Pkg(encoding)
	if err != nil {
		s.HandleHTTPError(r.Context(), &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "failed to parse package from provided URL",
			Err:  err,
		}, w)
		return
	}

	sum, diff, err := s.svc.DryRun(r.Context(), *orgID, userID, parsedPkg)
	if pkger.IsParseErr(err) {
		s.encJSONResp(r.Context(), w, http.StatusUnprocessableEntity, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
			Errors:  convertParseErr(err),
		})
		return
	}
	if err != nil {
		s.logger.Error("failed to dry run pkg", zap.Error(err))
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	// if only a dry run, then we exit before anything destructive
	if reqBody.DryRun {
		s.encJSONResp(r.Context(), w, http.StatusOK, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
		})
		return
	}

	sum, err = s.svc.Apply(r.Context(), *orgID, userID, parsedPkg, pkger.ApplyWithSecrets(reqBody.Secrets))
	if err != nil && !pkger.IsParseErr(err) {
		s.logger.Error("failed to apply pkg", zap.Error(err))
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	s.encJSONResp(r.Context(), w, http.StatusCreated, RespApplyPkg{
		Diff:    diff,
		Summary: sum,
		Errors:  convertParseErr(err),
	})
}

type encoder interface {
	Encode(interface{}) error
}

func decodeWithEncoding(r *http.Request, v interface{}) (pkger.Encoding, error) {
	encoding := pkgEncoding(r.Header)

	var dec interface{ Decode(interface{}) error }
	switch encoding {
	case pkger.EncodingJsonnet:
		dec = jsonnet.NewDecoder(r.Body)
	case pkger.EncodingYAML:
		dec = yaml.NewDecoder(r.Body)
	default:
		dec = json.NewDecoder(r.Body)
	}

	return encoding, dec.Decode(v)
}

func pkgEncoding(headers http.Header) pkger.Encoding {
	switch contentType := headers.Get("Content-Type"); contentType {
	case "application/x-jsonnet":
		return pkger.EncodingJsonnet
	case "text/yml", "application/x-yaml":
		return pkger.EncodingYAML
	default:
		return pkger.EncodingJSON
	}
}

func newJSONEnc(w io.Writer) encoder {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc
}

func (s *HandlerPkg) encResp(ctx context.Context, w http.ResponseWriter, enc encoder, code int, res interface{}) {
	w.WriteHeader(code)
	if err := enc.Encode(res); err != nil {
		s.HandleHTTPError(ctx, &influxdb.Error{
			Msg:  fmt.Sprintf("unable to marshal; Err: %v", err),
			Code: influxdb.EInternal,
			Err:  err,
		}, w)
	}
}

func (s *HandlerPkg) encJSONResp(ctx context.Context, w http.ResponseWriter, code int, res interface{}) {
	s.encResp(ctx, w, newJSONEnc(w), code, res)
}

// PkgerService provides an http client that is fluent in all things pkger.
type PkgerService struct {
	Client *httpc.Client
}

var _ pkger.SVC = (*PkgerService)(nil)

// CreatePkg will produce a pkg from the parameters provided.
func (s *PkgerService) CreatePkg(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
	var opt pkger.CreateOpt
	for _, setter := range setters {
		if err := setter(&opt); err != nil {
			return nil, err
		}
	}
	var orgIDs []string
	for orgID := range opt.OrgIDs {
		orgIDs = append(orgIDs, orgID.String())
	}

	reqBody := ReqCreatePkg{
		OrgIDs:    orgIDs,
		Resources: opt.Resources,
	}

	var newPkg *pkger.Pkg
	err := s.Client.
		PostJSON(reqBody, prefixPackages).
		Decode(func(resp *http.Response) error {
			pkg, err := pkger.Parse(pkger.EncodingJSON, pkger.FromReader(resp.Body))
			newPkg = pkg
			return err
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	if err := newPkg.Validate(pkger.ValidWithoutResources()); err != nil {
		return nil, err
	}
	return newPkg, nil
}

// DryRun provides a dry run of the pkg application. The pkg will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *PkgerService) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
	b, err := pkg.Encode(pkger.EncodingJSON)
	if err != nil {
		return pkger.Summary{}, pkger.Diff{}, err
	}

	reqBody := ReqApplyPkg{
		OrgID:  orgID.String(),
		DryRun: true,
		RawPkg: b,
	}
	return s.apply(ctx, reqBody)
}

// Apply will apply all the resources identified in the provided pkg. The entire pkg will be applied
// in its entirety. If a failure happens midway then the entire pkg will be rolled back to the state
// from before the pkg was applied.
func (s *PkgerService) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error) {
	var opt pkger.ApplyOpt
	for _, o := range opts {
		if err := o(&opt); err != nil {
			return pkger.Summary{}, err
		}
	}

	b, err := pkg.Encode(pkger.EncodingJSON)
	if err != nil {
		return pkger.Summary{}, err
	}

	reqBody := ReqApplyPkg{
		OrgID:   orgID.String(),
		Secrets: opt.MissingSecrets,
		RawPkg:  b,
	}

	sum, _, err := s.apply(ctx, reqBody)
	return sum, err
}

func (s *PkgerService) apply(ctx context.Context, reqBody ReqApplyPkg) (pkger.Summary, pkger.Diff, error) {
	var resp RespApplyPkg
	err := s.Client.
		PostJSON(reqBody, prefixPackages, "/apply").
		DecodeJSON(&resp).
		Do(ctx)
	if err != nil {
		return pkger.Summary{}, pkger.Diff{}, err
	}

	return resp.Summary, resp.Diff, pkger.NewParseError(resp.Errors...)
}

func convertParseErr(err error) []pkger.ValidationErr {
	pErr, ok := err.(pkger.ParseError)
	if !ok {
		return nil
	}
	return pErr.ValidationErrs()
}

func newDecodeErr(encoding string, err error) *influxdb.Error {
	return &influxdb.Error{
		Msg:  fmt.Sprintf("unable to unmarshal %s; Err: %v", encoding, err),
		Code: influxdb.EInvalid,
		Err:  err,
	}
}
