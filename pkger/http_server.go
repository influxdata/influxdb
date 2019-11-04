package pkger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb"
)

type HTTPServer struct {
	r chi.Router
	influxdb.HTTPErrorHandler
	svc *Service
}

func NewHTTPServer(errHandler influxdb.HTTPErrorHandler, svc *Service) *HTTPServer {
	svr := &HTTPServer{
		HTTPErrorHandler: errHandler,
		svc:              svc,
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Route("/api/v2/packages", func(r chi.Router) {
		r.Use(middleware.SetHeader("Content-Type", "application/json; charset=utf-8"))

		r.Post("/", svr.createPkg)
	})

	svr.r = r
	return svr
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.r.ServeHTTP(w, r)
}

type ReqCreatePkg struct {
	PkgName        string `json:"pkgName"`
	PkgDescription string `json:"pkgDescription"`
	PkgVersion     string `json:"pkgVersion"`
}

type RespCreatePkg struct {
	Package *Pkg `json:"package"`
}

func (s *HTTPServer) createPkg(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqCreatePkg
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		s.HandleHTTPError(r.Context(), newDecodeErr(err), w)
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

func (s *HTTPServer) encResp(ctx context.Context, w http.ResponseWriter, code int, res interface{}) {
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		s.HandleHTTPError(ctx, err, w)
	}
}

func newDecodeErr(err error) *influxdb.Error {
	return &influxdb.Error{
		Msg:  fmt.Sprintf("unable to marshal JSON; Err: %v", err),
		Code: influxdb.EInvalid,
		Err:  err,
	}
}
