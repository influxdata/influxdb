package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/pkger"
	"gopkg.in/yaml.v3"
)

// HandlerPkg is a server that manages the packages HTTP transport.
type HandlerPkg struct {
	chi.Router
	influxdb.HTTPErrorHandler
	svc pkger.SVC
}

// NewHandlerPkg constructs a new http server.
func NewHandlerPkg(errHandler influxdb.HTTPErrorHandler, svc pkger.SVC) *HandlerPkg {
	svr := &HandlerPkg{
		HTTPErrorHandler: errHandler,
		svc:              svc,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(traceMW)
	r.Use(middleware.Recoverer)

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
	return "/api/v2/packages"
}

type (
	// ReqCreatePkg is a request body for the create pkg endpoint.
	ReqCreatePkg struct {
		PkgName        string `json:"pkgName"`
		PkgDescription string `json:"pkgDescription"`
		PkgVersion     string `json:"pkgVersion"`

		Resources []pkger.ResourceToClone `json:"resources"`
	}

	// RespCreatePkg is a response body for the create pkg endpoint.
	RespCreatePkg struct {
		*pkger.Pkg
	}
)

func (s *HandlerPkg) createPkg(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqCreatePkg
	encoding, err := decodeWithEncoding(r, &reqBody)
	if err != nil {
		s.HandleHTTPError(r.Context(), newDecodeErr(encoding.String(), err), w)
		return
	}
	defer r.Body.Close()

	newPkg, err := s.svc.CreatePkg(r.Context(),
		pkger.CreateWithMetadata(pkger.Metadata{
			Description: reqBody.PkgDescription,
			Name:        reqBody.PkgName,
			Version:     reqBody.PkgVersion,
		}),
		pkger.CreateWithExistingResources(reqBody.Resources...),
	)
	if err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
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
	s.encResp(r.Context(), w, enc, http.StatusOK, RespCreatePkg{
		Pkg: newPkg,
	})
}

type (
	// ReqApplyPkg is the request body for a json or yaml body for the apply pkg endpoint.
	ReqApplyPkg struct {
		DryRun bool       `json:"dryRun" yaml:"dryRun"`
		OrgID  string     `json:"orgID" yaml:"orgID"`
		Pkg    *pkger.Pkg `json:"package" yaml:"package"`
	}

	// RespApplyPkg is the response body for the apply pkg endpoint.
	RespApplyPkg struct {
		Diff    pkger.Diff    `json:"diff" yaml:"diff"`
		Summary pkger.Summary `json:"summary" yaml:"summary"`

		Errors []pkger.ValidationErr `json:"errors,omitempty" yaml:"errors,omitempty"`
	}
)

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

	parsedPkg := reqBody.Pkg
	sum, diff, err := s.svc.DryRun(r.Context(), *orgID, parsedPkg)
	if pkger.IsParseErr(err) {
		s.encJSONResp(r.Context(), w, http.StatusUnprocessableEntity, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
			Errors:  convertParseErr(err),
		})
		return
	}
	if err != nil {
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

	sum, err = s.svc.Apply(r.Context(), *orgID, parsedPkg)
	if err != nil && !pkger.IsParseErr(err) {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	s.encJSONResp(r.Context(), w, http.StatusCreated, RespApplyPkg{
		Diff:    diff,
		Summary: sum,
	})
}

type encoder interface {
	Encode(interface{}) error
}

func decodeWithEncoding(r *http.Request, v interface{}) (pkger.Encoding, error) {
	var (
		encoding pkger.Encoding
		dec      interface{ Decode(interface{}) error }
	)
	switch contentType := r.Header.Get("Content-Type"); contentType {
	case "text/yml", "application/x-yaml":
		encoding = pkger.EncodingYAML
		dec = yaml.NewDecoder(r.Body)
	default:
		encoding = pkger.EncodingJSON
		dec = json.NewDecoder(r.Body)
	}

	return encoding, dec.Decode(v)
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

func traceMW(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		span, ctx := tracing.StartSpanFromContext(r.Context())
		defer span.Finish()
		next.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}
