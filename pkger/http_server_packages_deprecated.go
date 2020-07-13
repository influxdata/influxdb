package pkger

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	pctx "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

const RoutePrefixPackages = "/api/v2/packages"

// HTTPServerPackages is a server that manages the packages HTTP transport. These
// endpoints are to be sunset and replaced by the templates and stacks endpoints.
type HTTPServerPackages struct {
	chi.Router
	api    *kithttp.API
	logger *zap.Logger
	svc    SVC
}

// NewHTTPServerPackages constructs a new http server.
func NewHTTPServerPackages(log *zap.Logger, svc SVC) *HTTPServerPackages {
	svr := &HTTPServerPackages{
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		logger: log,
		svc:    svc,
	}

	exportAllowContentTypes := middleware.AllowContentType("text/yml", "application/x-yaml", "application/json")
	setJSONContentType := middleware.SetHeader("Content-Type", "application/json; charset=utf-8")

	r := chi.NewRouter()
	r.Use(
		middleware.SetHeader("Sunset", "Thurs, 30 July 2020 17:00:00 UTC"),
	)
	{
		r.With(exportAllowContentTypes).Post("/", svr.export)
		r.With(setJSONContentType).Post("/apply", svr.apply)

		r.Route("/stacks", func(r chi.Router) {
			r.Post("/", svr.createStack)
			r.Get("/", svr.listStacks)

			r.Route("/{stack_id}", func(r chi.Router) {
				r.Get("/", svr.readStack)
				r.Delete("/", svr.deleteStack)
				r.Patch("/", svr.updateStack)
				r.With(exportAllowContentTypes).Get("/export", svr.exportStack)
			})
		})
	}

	svr.Router = r
	return svr
}

// Prefix provides the prefix to this route tree.
func (s *HTTPServerPackages) Prefix() string {
	return RoutePrefixPackages
}

func (s *HTTPServerPackages) listStacks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	rawOrgID := q.Get("orgID")
	orgID, err := influxdb.IDFromString(rawOrgID)
	if err != nil {
		s.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("organization id[%q] is invalid", rawOrgID),
			Err:  err,
		})
		return
	}

	if err := r.ParseForm(); err != nil {
		s.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "failed to parse form from encoded url",
			Err:  err,
		})
		return
	}

	filter := ListFilter{
		Names: r.Form["name"],
	}

	for _, idRaw := range r.Form["stackID"] {
		id, err := influxdb.IDFromString(idRaw)
		if err != nil {
			s.api.Err(w, r, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("stack ID[%q] provided is invalid", idRaw),
				Err:  err,
			})
			return
		}
		filter.StackIDs = append(filter.StackIDs, *id)
	}

	stacks, err := s.svc.ListStacks(r.Context(), *orgID, filter)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}
	if stacks == nil {
		stacks = []Stack{}
	}

	out := make([]RespStack, 0, len(stacks))
	for _, st := range stacks {
		out = append(out, convertStackToRespStack(st))
	}

	s.api.Respond(w, r, http.StatusOK, RespListStacks{
		Stacks: out,
	})
}

func (s *HTTPServerPackages) createStack(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqCreateStack
	if err := s.api.DecodeJSON(r.Body, &reqBody); err != nil {
		s.api.Err(w, r, err)
		return
	}
	defer r.Body.Close()

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	stack, err := s.svc.InitStack(r.Context(), auth.GetUserID(), StackCreate{
		OrgID:        reqBody.orgID(),
		Name:         reqBody.Name,
		Description:  reqBody.Description,
		TemplateURLs: reqBody.URLs,
	})
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusCreated, convertStackToRespStack(stack))
}

func (s *HTTPServerPackages) deleteStack(w http.ResponseWriter, r *http.Request) {
	orgID, err := getRequiredOrgIDFromQuery(r.URL.Query())
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	stackID, err := stackIDFromReq(r)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.api.Err(w, r, err)
		return
	}
	userID := auth.GetUserID()

	err = s.svc.DeleteStack(r.Context(), struct{ OrgID, UserID, StackID influxdb.ID }{
		OrgID:   orgID,
		UserID:  userID,
		StackID: stackID,
	})
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusNoContent, nil)
}

func (s *HTTPServerPackages) exportStack(w http.ResponseWriter, r *http.Request) {
	stackID, err := stackIDFromReq(r)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	pkg, err := s.svc.Export(r.Context(), ExportWithStackID(stackID))
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	encoding := templateEncoding(r.Header.Get("Accept"))

	b, err := pkg.Encode(encoding)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	switch encoding {
	case EncodingYAML:
		w.Header().Set("Content-Type", "application/x-yaml")
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	s.api.Write(w, http.StatusOK, b)
}

func (s *HTTPServerPackages) readStack(w http.ResponseWriter, r *http.Request) {
	stackID, err := stackIDFromReq(r)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	stack, err := s.svc.ReadStack(r.Context(), stackID)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusOK, convertStackToRespStack(stack))
}

func (s *HTTPServerPackages) updateStack(w http.ResponseWriter, r *http.Request) {
	var req ReqUpdateStack
	if err := s.api.DecodeJSON(r.Body, &req); err != nil {
		s.api.Err(w, r, err)
		return
	}

	stackID, err := stackIDFromReq(r)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	update := StackUpdate{
		ID:           stackID,
		Name:         req.Name,
		Description:  req.Description,
		TemplateURLs: append(req.TemplateURLs, req.URLs...),
	}
	for _, res := range req.AdditionalResources {
		id, err := influxdb.IDFromString(res.ID)
		if err != nil {
			s.api.Err(w, r, influxErr(influxdb.EInvalid, err, fmt.Sprintf("stack resource id %q", res.ID)))
			return
		}
		update.AdditionalResources = append(update.AdditionalResources, StackAdditionalResource{
			APIVersion: APIVersion,
			ID:         *id,
			Kind:       res.Kind,
			MetaName:   res.MetaName,
		})
	}

	stack, err := s.svc.UpdateStack(r.Context(), update)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusOK, convertStackToRespStack(stack))
}

func (s *HTTPServerPackages) export(w http.ResponseWriter, r *http.Request) {
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
		orgID, err := influxdb.IDFromString(orgIDStr.OrgID)
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
		stackID, err := influxdb.IDFromString(reqBody.StackID)
		if err != nil {
			s.api.Err(w, r, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("invalid stack ID provided: %q", reqBody.StackID),
			})
			return
		}
		opts = append(opts, ExportWithStackID(*stackID))
	}

	newPkg, err := s.svc.Export(r.Context(), opts...)
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	resp := RespExport(newPkg.Objects)
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

func (s *HTTPServerPackages) apply(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqApply
	encoding, err := decodeWithEncoding(r, &reqBody)
	if err != nil {
		s.api.Err(w, r, newDecodeErr(encoding.String(), err))
		return
	}

	orgID, err := influxdb.IDFromString(reqBody.OrgID)
	if err != nil {
		s.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid organization ID provided: %q", reqBody.OrgID),
		})
		return
	}

	var stackID influxdb.ID
	if reqBody.StackID != nil {
		if err := stackID.DecodeFromString(*reqBody.StackID); err != nil {
			s.api.Err(w, r, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  fmt.Sprintf("invalid stack ID provided: %q", *reqBody.StackID),
			})
			return
		}
	}

	parsedPkg, err := reqBody.Templates(encoding)
	if err != nil {
		s.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
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
		ApplyWithTemplate(parsedPkg),
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

func (s *HTTPServerPackages) encResp(w http.ResponseWriter, r *http.Request, enc encoder, code int, res interface{}) {
	w.WriteHeader(code)
	if err := enc.Encode(res); err != nil {
		s.api.Err(w, r, &influxdb.Error{
			Msg:  fmt.Sprintf("unable to marshal; Err: %v", err),
			Code: influxdb.EInternal,
			Err:  err,
		})
	}
}
