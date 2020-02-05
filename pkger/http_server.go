package pkger

import (
	"bytes"
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
	kithttp "github.com/influxdata/influxdb/kit/transport/http"
	"github.com/influxdata/influxdb/pkg/jsonnet"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const RoutePrefix = "/api/v2/packages"

// HTTPServer is a server that manages the packages HTTP transport.
type HTTPServer struct {
	chi.Router
	api    *kithttp.API
	logger *zap.Logger
	svc    SVC
}

// NewHTTPServer constructs a new http server.
func NewHTTPServer(log *zap.Logger, svc SVC) *HTTPServer {
	svr := &HTTPServer{
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		logger: log,
		svc:    svc,
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
func (s *HTTPServer) Prefix() string {
	return RoutePrefix
}

type (
	// ReqCreatePkg is a request body for the create pkg endpoint.
	ReqCreatePkg struct {
		OrgIDs    []string          `json:"orgIDs"`
		Resources []ResourceToClone `json:"resources"`
	}

	// RespCreatePkg is a response body for the create pkg endpoint.
	RespCreatePkg []Object
)

func (s *HTTPServer) createPkg(w http.ResponseWriter, r *http.Request) {
	encoding := pkgEncoding(r.Header)

	var reqBody ReqCreatePkg
	if err := s.api.DecodeJSON(r.Body, &reqBody); err != nil {
		s.api.Err(w, err)
		return
	}
	defer r.Body.Close()

	if len(reqBody.Resources) == 0 && len(reqBody.OrgIDs) == 0 {
		s.api.Err(w, &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "at least 1 resource or 1 org id must be provided",
		})
		return
	}

	opts := []CreatePkgSetFn{
		CreateWithExistingResources(reqBody.Resources...),
	}
	for _, orgIDStr := range reqBody.OrgIDs {
		orgID, err := influxdb.IDFromString(orgIDStr)
		if err != nil {
			continue
		}
		opts = append(opts, CreateWithAllOrgResources(*orgID))
	}

	newPkg, err := s.svc.CreatePkg(r.Context(), opts...)
	if err != nil {
		s.api.Err(w, err)
		return
	}

	resp := RespCreatePkg(newPkg.Objects)
	if resp == nil {
		resp = []Object{}
	}

	var enc encoder
	switch encoding {
	case EncodingYAML:
		enc = yaml.NewEncoder(w)
		w.Header().Set("Content-Type", "application/x-yaml")
	default:
		enc = newJSONEnc(w)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	s.encResp(w, enc, http.StatusOK, resp)
}

// PkgRemote provides a package via a remote (i.e. a gist). If content type is not
// provided then the service will do its best to discern the content type of the
// contents.
type PkgRemote struct {
	URL         string `json:"url"`
	ContentType string `json:"contentType"`
}

// Encoding returns the encoding type that corresponds to the given content type.
func (p PkgRemote) Encoding() Encoding {
	ct := strings.ToLower(p.ContentType)
	urlBase := path.Ext(p.URL)
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

// ReqApplyPkg is the request body for a json or yaml body for the apply pkg endpoint.
type ReqApplyPkg struct {
	DryRun  bool              `json:"dryRun" yaml:"dryRun"`
	OrgID   string            `json:"orgID" yaml:"orgID"`
	Remote  PkgRemote         `json:"remote" yaml:"remote"`
	RawPkg  json.RawMessage   `json:"package" yaml:"package"`
	EnvRefs map[string]string `json:"envRefs"`
	Secrets map[string]string `json:"secrets"`
}

// Pkg returns a pkg parsed and validated from the RawPkg field.
func (r ReqApplyPkg) Pkg(encoding Encoding) (*Pkg, error) {
	if r.Remote.URL != "" {
		return Parse(r.Remote.Encoding(), FromHTTPRequest(r.Remote.URL))
	}

	return Parse(encoding, FromReader(bytes.NewReader(r.RawPkg)))
}

// RespApplyPkg is the response body for the apply pkg endpoint.
type RespApplyPkg struct {
	Diff    Diff    `json:"diff" yaml:"diff"`
	Summary Summary `json:"summary" yaml:"summary"`

	Errors []ValidationErr `json:"errors,omitempty" yaml:"errors,omitempty"`
}

func (s *HTTPServer) applyPkg(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqApplyPkg
	encoding, err := decodeWithEncoding(r, &reqBody)
	if err != nil {
		s.api.Err(w, newDecodeErr(encoding.String(), err))
		return
	}

	orgID, err := influxdb.IDFromString(reqBody.OrgID)
	if err != nil {
		s.api.Err(w, &influxdb.Error{
			Code: influxdb.EConflict,
			Msg:  fmt.Sprintf("invalid organization ID provided: %q", reqBody.OrgID),
		})
		return
	}

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.api.Err(w, err)
		return
	}
	userID := auth.GetUserID()

	parsedPkg, err := reqBody.Pkg(encoding)
	if err != nil {
		s.api.Err(w, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		})
		return
	}

	sum, diff, err := s.svc.DryRun(r.Context(), *orgID, userID, parsedPkg)
	if IsParseErr(err) {
		s.api.Respond(w, http.StatusUnprocessableEntity, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
			Errors:  convertParseErr(err),
		})
		return
	}
	if err != nil {
		s.api.Err(w, err)
		return
	}

	// if only a dry run, then we exit before anything destructive
	if reqBody.DryRun {
		s.api.Respond(w, http.StatusOK, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
		})
		return
	}

	sum, err = s.svc.Apply(r.Context(), *orgID, userID, parsedPkg, ApplyWithEnvRefs(reqBody.EnvRefs), ApplyWithSecrets(reqBody.Secrets))
	if err != nil && !IsParseErr(err) {
		s.api.Err(w, err)
		return
	}

	s.api.Respond(w, http.StatusCreated, RespApplyPkg{
		Diff:    diff,
		Summary: sum,
		Errors:  convertParseErr(err),
	})
}

type encoder interface {
	Encode(interface{}) error
}

func decodeWithEncoding(r *http.Request, v interface{}) (Encoding, error) {
	encoding := pkgEncoding(r.Header)

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

func pkgEncoding(headers http.Header) Encoding {
	switch contentType := headers.Get("Content-Type"); contentType {
	case "application/x-jsonnet":
		return EncodingJsonnet
	case "text/yml", "application/x-yaml":
		return EncodingYAML
	default:
		return EncodingJSON
	}
}

func newJSONEnc(w io.Writer) encoder {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc
}

func (s *HTTPServer) encResp(w http.ResponseWriter, enc encoder, code int, res interface{}) {
	w.WriteHeader(code)
	if err := enc.Encode(res); err != nil {
		s.api.Err(w, &influxdb.Error{
			Msg:  fmt.Sprintf("unable to marshal; Err: %v", err),
			Code: influxdb.EInternal,
			Err:  err,
		})
	}
}

func convertParseErr(err error) []ValidationErr {
	pErr, ok := err.(ParseError)
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
