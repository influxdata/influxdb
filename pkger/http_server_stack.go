package pkger

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/go-chi/chi"
	pctx "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const RoutePrefixStacks = "/api/v2/stacks"

// HTTPServerStacks is a server that manages the stacks HTTP transport.
type HTTPServerStacks struct {
	chi.Router
	api    *kithttp.API
	logger *zap.Logger
	svc    SVC
}

// NewHTTPServerStacks constructs a new http server.
func NewHTTPServerStacks(log *zap.Logger, svc SVC) *HTTPServerStacks {
	svr := &HTTPServerStacks{
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		logger: log,
		svc:    svc,
	}

	r := chi.NewRouter()
	{
		r.Post("/", svr.createStack)
		r.Get("/", svr.listStacks)

		r.Route("/{stack_id}", func(r chi.Router) {
			r.Get("/", svr.readStack)
			r.Delete("/", svr.deleteStack)
			r.Patch("/", svr.updateStack)
			r.Post("/uninstall", svr.uninstallStack)
		})
	}

	svr.Router = r
	return svr
}

// Prefix provides the prefix to this route tree.
func (s *HTTPServerStacks) Prefix() string {
	return RoutePrefixStacks
}

type (
	// RespStack is the response body for a stack.
	RespStack struct {
		ID        string           `json:"id"`
		OrgID     string           `json:"orgID"`
		CreatedAt time.Time        `json:"createdAt"`
		Events    []RespStackEvent `json:"events"`

		// maintain same interface for backward compatibility
		RespStackEvent
	}

	RespStackEvent struct {
		EventType   string              `json:"eventType"`
		Name        string              `json:"name"`
		Description string              `json:"description"`
		Resources   []RespStackResource `json:"resources"`
		Sources     []string            `json:"sources"`
		URLs        []string            `json:"urls"`
		UpdatedAt   time.Time           `json:"updatedAt"`
	}

	// RespStackResource is the response for a stack resource. This type exists
	// to decouple the internal service implementation from the deprecates usage
	// of templates in the API. We could add a custom UnmarshalJSON method, but
	// I would rather keep it obvious and explicit with a separate field.
	RespStackResource struct {
		APIVersion   string                   `json:"apiVersion"`
		ID           string                   `json:"resourceID"`
		Kind         Kind                     `json:"kind"`
		MetaName     string                   `json:"templateMetaName"`
		Associations []RespStackResourceAssoc `json:"associations"`
		Links        RespStackResourceLinks   `json:"links"`
	}

	// RespStackResourceAssoc is the response for a stack resource's associations.
	RespStackResourceAssoc struct {
		Kind     Kind   `json:"kind"`
		MetaName string `json:"metaName"`
	}

	RespStackResourceLinks struct {
		Self string `json:"self"`
	}
)

// RespListStacks is the HTTP response for a stack list call.
type RespListStacks struct {
	Stacks []RespStack `json:"stacks"`
}

func (s *HTTPServerStacks) listStacks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	rawOrgID := q.Get("orgID")
	orgID, err := platform.IDFromString(rawOrgID)
	if err != nil {
		s.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("organization id[%q] is invalid", rawOrgID),
			Err:  err,
		})
		return
	}

	if err := r.ParseForm(); err != nil {
		s.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "failed to parse form from encoded url",
			Err:  err,
		})
		return
	}

	filter := ListFilter{
		Names: r.Form["name"],
	}

	for _, idRaw := range r.Form["stackID"] {
		id, err := platform.IDFromString(idRaw)
		if err != nil {
			s.api.Err(w, r, &errors.Error{
				Code: errors.EInvalid,
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

// ReqCreateStack is a request body for a create stack call.
type ReqCreateStack struct {
	OrgID       string   `json:"orgID"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	URLs        []string `json:"urls"`
}

// OK validates the request body is valid.
func (r *ReqCreateStack) OK() error {
	// TODO: provide multiple errors back for failing validation
	if _, err := platform.IDFromString(r.OrgID); err != nil {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  fmt.Sprintf("provided org id[%q] is invalid", r.OrgID),
		}
	}

	for _, u := range r.URLs {
		if _, err := url.Parse(u); err != nil {
			return &errors.Error{
				Code: errors.EInvalid,
				Msg:  fmt.Sprintf("provided url[%q] is invalid", u),
			}
		}
	}
	return nil
}

func (r *ReqCreateStack) orgID() platform.ID {
	orgID, _ := platform.IDFromString(r.OrgID)
	return *orgID
}

func (s *HTTPServerStacks) createStack(w http.ResponseWriter, r *http.Request) {
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

func (s *HTTPServerStacks) deleteStack(w http.ResponseWriter, r *http.Request) {
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

	err = s.svc.DeleteStack(r.Context(), struct{ OrgID, UserID, StackID platform.ID }{
		OrgID:   orgID,
		UserID:  auth.GetUserID(),
		StackID: stackID,
	})
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusNoContent, nil)
}

func (s *HTTPServerStacks) uninstallStack(w http.ResponseWriter, r *http.Request) {
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

	stack, err := s.svc.UninstallStack(r.Context(), struct{ OrgID, UserID, StackID platform.ID }{
		OrgID:   orgID,
		UserID:  auth.GetUserID(),
		StackID: stackID,
	})
	if err != nil {
		s.api.Err(w, r, err)
		return
	}

	s.api.Respond(w, r, http.StatusOK, convertStackToRespStack(stack))
}

func (s *HTTPServerStacks) readStack(w http.ResponseWriter, r *http.Request) {
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

type (
	// ReqUpdateStack is the request body for updating a stack.
	ReqUpdateStack struct {
		Name                *string                  `json:"name"`
		Description         *string                  `json:"description"`
		TemplateURLs        []string                 `json:"templateURLs"`
		AdditionalResources []ReqUpdateStackResource `json:"additionalResources"`

		// Deprecating the urls field and replacing with templateURLs field.
		// This is remaining here for backwards compatibility.
		URLs []string `json:"urls"`
	}

	ReqUpdateStackResource struct {
		ID       string `json:"resourceID"`
		Kind     Kind   `json:"kind"`
		MetaName string `json:"templateMetaName"`
	}
)

func (s *HTTPServerStacks) updateStack(w http.ResponseWriter, r *http.Request) {
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
		id, err := platform.IDFromString(res.ID)
		if err != nil {
			s.api.Err(w, r, influxErr(errors.EInvalid, err, fmt.Sprintf("stack resource id %q", res.ID)))
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

func convertStackToRespStack(st Stack) RespStack {
	events := make([]RespStackEvent, 0, len(st.Events))
	for _, ev := range st.Events {
		events = append(events, convertStackEvent(ev))
	}

	return RespStack{
		ID:             st.ID.String(),
		OrgID:          st.OrgID.String(),
		CreatedAt:      st.CreatedAt,
		RespStackEvent: convertStackEvent(st.LatestEvent()),
		Events:         events,
	}
}

func convertStackEvent(ev StackEvent) RespStackEvent {
	resources := make([]RespStackResource, 0, len(ev.Resources))
	for _, r := range ev.Resources {
		asses := make([]RespStackResourceAssoc, 0, len(r.Associations))
		for _, a := range r.Associations {
			asses = append(asses, RespStackResourceAssoc(a))
		}
		resources = append(resources, RespStackResource{
			APIVersion:   r.APIVersion,
			ID:           r.ID.String(),
			Kind:         r.Kind,
			MetaName:     r.MetaName,
			Links:        stackResLinks(r),
			Associations: asses,
		})
	}

	return RespStackEvent{
		EventType:   ev.EventType.String(),
		Name:        ev.Name,
		Description: ev.Description,
		Resources:   resources,
		Sources:     append([]string{}, ev.Sources...),
		URLs:        append([]string{}, ev.TemplateURLs...),
		UpdatedAt:   ev.UpdatedAt,
	}
}

func stackResLinks(r StackResource) RespStackResourceLinks {
	var linkResource string
	switch r.Kind {
	case KindBucket:
		linkResource = "buckets"
	case KindCheck, KindCheckDeadman, KindCheckThreshold:
		linkResource = "checks"
	case KindDashboard:
		linkResource = "dashboards"
	case KindLabel:
		linkResource = "labels"
	case KindNotificationEndpoint,
		KindNotificationEndpointHTTP,
		KindNotificationEndpointPagerDuty,
		KindNotificationEndpointSlack:
		linkResource = "notificationEndpoints"
	case KindNotificationRule:
		linkResource = "notificationRules"
	case KindTask:
		linkResource = "tasks"
	case KindTelegraf:
		linkResource = "telegrafs"
	case KindVariable:
		linkResource = "variables"
	}
	return RespStackResourceLinks{
		Self: path.Join("/api/v2", linkResource, r.ID.String()),
	}
}

func stackIDFromReq(r *http.Request) (platform.ID, error) {
	stackID, err := platform.IDFromString(chi.URLParam(r, "stack_id"))
	if err != nil {
		return 0, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "the stack id provided in the path was invalid",
			Err:  err,
		}
	}
	return *stackID, nil
}

func getRequiredOrgIDFromQuery(q url.Values) (platform.ID, error) {
	orgIDRaw := q.Get("orgID")
	if orgIDRaw == "" {
		return 0, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "the orgID query param is required",
		}
	}

	orgID, err := platform.IDFromString(orgIDRaw)
	if err != nil {
		return 0, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "the orgID query param was invalid",
			Err:  err,
		}
	}
	return *orgID, nil
}
