package pkger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	"gopkg.in/yaml.v3"
)

// HTTPServer is a server that manages the packages HTTP transport.
type HTTPServer struct {
	r chi.Router
	influxdb.HTTPErrorHandler
	svc SVC
}

// NewHTTPServer constructs a new http server.
func NewHTTPServer(errHandler influxdb.HTTPErrorHandler, svc SVC) *HTTPServer {
	svr := &HTTPServer{
		HTTPErrorHandler: errHandler,
		svc:              svc,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(traceMW)
	r.Use(middleware.SetHeader("Content-Type", "application/json; charset=utf-8"))
	r.Use(middleware.Recoverer)

	r.Route(svr.Prefix(), func(r chi.Router) {
		r.Post("/", svr.createPkg)
		r.Post("/apply", svr.applyPkg)
	})

	svr.r = r
	return svr
}

func (s *HTTPServer) Prefix() string {
	return "/api/v2/packages"
}

// ServeHTTP serves up the http request.
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.r.ServeHTTP(w, r)
}

// ReqCreatePkg is a request body for the create pkg endpoint.
type ReqCreatePkg struct {
	PkgName        string `json:"pkgName"`
	PkgDescription string `json:"pkgDescription"`
	PkgVersion     string `json:"pkgVersion"`
}

// RespCreatePkg is a response body for the create pkg endpoint.
type RespCreatePkg struct {
	Package *Pkg `json:"package"`
}

func (s *HTTPServer) createPkg(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqCreatePkg
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		s.HandleHTTPError(r.Context(), newDecodeErr("json", err), w)
		return
	}
	defer r.Body.Close()

	newPkg, err := s.svc.CreatePkg(r.Context(),
		WithMetadata(Metadata{
			Description: reqBody.PkgDescription,
			Name:        reqBody.PkgName,
			Version:     reqBody.PkgVersion,
		}),
	)
	if err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	s.encResp(r.Context(), w, http.StatusOK, RespCreatePkg{
		Package: newPkg,
	})
}

// ReqApplyPkg is the request body for a json or yaml body for the apply pkg endpoint.
type ReqApplyPkg struct {
	DryRun bool   `yaml:"dryRun" json:"dryRun"`
	OrgID  string `yaml:"orgID" json:"orgID"`
	Pkg    *Pkg   `yaml:"package" json:"package"`
}

// RespApplyPkg is the response body for the apply pkg endpoint.
type RespApplyPkg struct {
	Diff    Diff    `yaml:"diff" json:"diff"`
	Summary Summary `yaml:"summary" json:"summary"`
}

func (s *HTTPServer) applyPkg(w http.ResponseWriter, r *http.Request) {
	reqBody, err := decodeApplyReq(r)
	if err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	orgID, err := influxdb.IDFromString(reqBody.OrgID)
	if err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	if err := reqBody.Pkg.Validate(); err != nil {
		s.HandleHTTPError(r.Context(), err, w)
		return
	}

	parsedPkg := reqBody.Pkg
	sum, diff, err := s.svc.DryRun(r.Context(), *orgID, parsedPkg)
	if err != nil {
		s.HandleHTTPError(r.Context(), httpParseErr(err), w)
		return
	}

	// if only a dry run, then we exit before anything destructive
	if reqBody.DryRun {
		s.encResp(r.Context(), w, http.StatusOK, RespApplyPkg{
			Diff:    diff,
			Summary: sum,
		})
		return
	}

	sum, err = s.svc.Apply(r.Context(), *orgID, parsedPkg)
	if err != nil {
		s.HandleHTTPError(r.Context(), httpParseErr(err), w)
		return
	}

	s.encResp(r.Context(), w, http.StatusCreated, RespApplyPkg{
		Diff:    diff,
		Summary: sum,
	})
}

func decodeApplyReq(r *http.Request) (ReqApplyPkg, error) {
	var (
		reqBody  ReqApplyPkg
		encoding Encoding
		err      error
	)

	switch contentType := r.Header.Get("Content-Type"); contentType {
	case "text/yml", "application/x-yaml":
		encoding = EncodingYAML
		err = yaml.NewDecoder(r.Body).Decode(&reqBody)
	default:
		encoding = EncodingJSON
		err = json.NewDecoder(r.Body).Decode(&reqBody)
	}
	if err != nil {
		return ReqApplyPkg{}, newDecodeErr(encoding.String(), err)
	}

	return reqBody, nil
}

func (s *HTTPServer) encResp(ctx context.Context, w http.ResponseWriter, code int, res interface{}) {
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	if err := enc.Encode(res); err != nil {
		s.HandleHTTPError(ctx, &influxdb.Error{
			Msg:  fmt.Sprintf("unable to marshal json; Err: %v", err),
			Code: influxdb.EInternal,
			Err:  err,
		}, w)
	}
}

func newDecodeErr(encoding string, err error) *influxdb.Error {
	return &influxdb.Error{
		Msg:  fmt.Sprintf("unable to unmarshal %s; Err: %v", encoding, err),
		Code: influxdb.EInvalid,
		Err:  err,
	}
}

func httpParseErr(err error) error {
	return err
}

func traceMW(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		span, ctx := tracing.StartSpanFromContext(r.Context())
		defer span.Finish()
		next.ServeHTTP(w, r.WithContext(ctx))
	}
	return http.HandlerFunc(fn)
}
