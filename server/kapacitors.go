package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	kapa "github.com/influxdata/chronograf/kapacitor"
)

type postKapacitorRequest struct {
	Name         *string `json:"name"`               // User facing name of kapacitor instance.; Required: true
	URL          *string `json:"url"`                // URL for the kapacitor backend (e.g. http://localhost:9092);/ Required: true
	Username     string  `json:"username,omitempty"` // Username for authentication to kapacitor
	Password     string  `json:"password,omitempty"`
	Active       bool    `json:"active"`
	Organization string  `json:"organization"` // Organization is the organization ID that resource belongs to
}

func (p *postKapacitorRequest) Valid() error {
	if p.Name == nil || p.URL == nil {
		return fmt.Errorf("name and url required")
	}

	if p.Organization == "" {
		return fmt.Errorf("organization must be set")
	}

	url, err := url.ParseRequestURI(*p.URL)
	if err != nil {
		return fmt.Errorf("invalid source URI: %v", err)
	}
	if len(url.Scheme) == 0 {
		return fmt.Errorf("Invalid URL; no URL scheme defined")
	}

	return nil
}

type kapaLinks struct {
	Proxy string `json:"proxy"` // URL location of proxy endpoint for this source
	Self  string `json:"self"`  // Self link mapping to this resource
	Rules string `json:"rules"` // Rules link for defining roles alerts for kapacitor
	Tasks string `json:"tasks"` // Tasks link to define a task against the proxy
	Ping  string `json:"ping"`  // Ping path to kapacitor
}

type kapacitor struct {
	ID       int       `json:"id,string"`          // Unique identifier representing a kapacitor instance.
	Name     string    `json:"name"`               // User facing name of kapacitor instance.
	URL      string    `json:"url"`                // URL for the kapacitor backend (e.g. http://localhost:9092)
	Username string    `json:"username,omitempty"` // Username for authentication to kapacitor
	Password string    `json:"password,omitempty"`
	Active   bool      `json:"active"`
	Links    kapaLinks `json:"links"` // Links are URI locations related to kapacitor
}

// NewKapacitor adds valid kapacitor store store.
func (s *Service) NewKapacitor(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	_, err = s.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, s.Logger)
		return
	}

	var req postKapacitorRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	srv := chronograf.Server{
		SrcID:        srcID,
		Name:         *req.Name,
		Username:     req.Username,
		Password:     req.Password,
		URL:          *req.URL,
		Active:       req.Active,
		Organization: req.Organization,
	}

	if srv, err = s.ServersStore.Add(ctx, srv); err != nil {
		msg := fmt.Errorf("Error storing kapacitor %v: %v", req, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newKapacitor(srv)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, s.Logger)
}

func newKapacitor(srv chronograf.Server) kapacitor {
	httpAPISrcs := "/chronograf/v1/sources"
	return kapacitor{
		ID:       srv.ID,
		Name:     srv.Name,
		Username: srv.Username,
		URL:      srv.URL,
		Active:   srv.Active,
		Links: kapaLinks{
			Self:  fmt.Sprintf("%s/%d/kapacitors/%d", httpAPISrcs, srv.SrcID, srv.ID),
			Proxy: fmt.Sprintf("%s/%d/kapacitors/%d/proxy", httpAPISrcs, srv.SrcID, srv.ID),
			Rules: fmt.Sprintf("%s/%d/kapacitors/%d/rules", httpAPISrcs, srv.SrcID, srv.ID),
			Tasks: fmt.Sprintf("%s/%d/kapacitors/%d/proxy?path=/kapacitor/v1/tasks", httpAPISrcs, srv.SrcID, srv.ID),
			Ping:  fmt.Sprintf("%s/%d/kapacitors/%d/proxy?path=/kapacitor/v1/ping", httpAPISrcs, srv.SrcID, srv.ID),
		},
	}
}

type kapacitors struct {
	Kapacitors []kapacitor `json:"kapacitors"`
}

// Kapacitors retrieves all kapacitors from store.
func (s *Service) Kapacitors(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	mrSrvs, err := s.ServersStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading kapacitors", s.Logger)
		return
	}

	srvs := []kapacitor{}
	for _, srv := range mrSrvs {
		if srv.SrcID == srcID {
			srvs = append(srvs, newKapacitor(srv))
		}
	}

	res := kapacitors{
		Kapacitors: srvs,
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// KapacitorsID retrieves a kapacitor with ID from store.
func (s *Service) KapacitorsID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	res := newKapacitor(srv)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// RemoveKapacitor deletes kapacitor from store.
func (s *Service) RemoveKapacitor(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	if err = s.ServersStore.Delete(ctx, srv); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type patchKapacitorRequest struct {
	Name     *string `json:"name,omitempty"`     // User facing name of kapacitor instance.
	URL      *string `json:"url,omitempty"`      // URL for the kapacitor
	Username *string `json:"username,omitempty"` // Username for kapacitor auth
	Password *string `json:"password,omitempty"`
	Active   *bool   `json:"active"`
}

func (p *patchKapacitorRequest) Valid() error {
	if p.URL != nil {
		url, err := url.ParseRequestURI(*p.URL)
		if err != nil {
			return fmt.Errorf("invalid source URI: %v", err)
		}
		if len(url.Scheme) == 0 {
			return fmt.Errorf("Invalid URL; no URL scheme defined")
		}
	}
	return nil
}

// UpdateKapacitor incrementally updates a kapacitor definition in the store
func (s *Service) UpdateKapacitor(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	var req patchKapacitorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	if req.Name != nil {
		srv.Name = *req.Name
	}
	if req.URL != nil {
		srv.URL = *req.URL
	}
	if req.Password != nil {
		srv.Password = *req.Password
	}
	if req.Username != nil {
		srv.Username = *req.Username
	}
	if req.Active != nil {
		srv.Active = *req.Active
	}

	if err := s.ServersStore.Update(ctx, srv); err != nil {
		msg := fmt.Sprintf("Error updating kapacitor ID %d", id)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	res := newKapacitor(srv)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// KapacitorRulesPost proxies POST to kapacitor
func (s *Service) KapacitorRulesPost(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)

	var req chronograf.AlertRule
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	// TODO: validate this data
	/*
		if err := req.Valid(); err != nil {
			invalidData(w, err)
			return
		}
	*/

	task, err := c.Create(ctx, req)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}
	res := newAlertResponse(task, srv.SrcID, srv.ID)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, s.Logger)
}

type alertLinks struct {
	Self      string `json:"self"`
	Kapacitor string `json:"kapacitor"`
	Output    string `json:"output"`
}

type alertResponse struct {
	chronograf.AlertRule
	Links alertLinks `json:"links"`
}

// newAlertResponse formats task into an alertResponse
func newAlertResponse(task *kapa.Task, srcID, kapaID int) *alertResponse {
	res := &alertResponse{
		AlertRule: task.Rule,
		Links: alertLinks{
			Self:      fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/rules/%s", srcID, kapaID, task.ID),
			Kapacitor: fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/proxy?path=%s", srcID, kapaID, url.QueryEscape(task.Href)),
			Output:    fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/proxy?path=%s", srcID, kapaID, url.QueryEscape(task.HrefOutput)),
		},
	}

	if res.Alerts == nil {
		res.Alerts = make([]string, 0)
	}

	if res.AlertNodes == nil {
		res.AlertNodes = make([]chronograf.KapacitorNode, 0)
	}

	for _, n := range res.AlertNodes {
		if n.Args == nil {
			n.Args = make([]string, 0)
		}
		if n.Properties == nil {
			n.Properties = make([]chronograf.KapacitorProperty, 0)
		}
		for _, p := range n.Properties {
			if p.Args == nil {
				p.Args = make([]string, 0)
			}
		}
	}

	if res.Query != nil {
		if res.Query.ID == "" {
			res.Query.ID = res.ID
		}

		if res.Query.Fields == nil {
			res.Query.Fields = make([]chronograf.Field, 0)
		}

		if res.Query.GroupBy.Tags == nil {
			res.Query.GroupBy.Tags = make([]string, 0)
		}

		if res.Query.Tags == nil {
			res.Query.Tags = make(map[string][]string)
		}
	}
	return res
}

// ValidRuleRequest checks if the requested rule change is valid
func ValidRuleRequest(rule chronograf.AlertRule) error {
	if rule.Query == nil {
		return fmt.Errorf("invalid alert rule: no query defined")
	}
	var hasFuncs bool
	for _, f := range rule.Query.Fields {
		if f.Type == "func" && len(f.Args) > 0 {
			hasFuncs = true
		}
	}
	// All kapacitor rules with functions must have a window that is applied
	// every amount of time
	if rule.Every == "" && hasFuncs {
		return fmt.Errorf(`invalid alert rule: functions require an "every" window`)
	}
	return nil
}

// KapacitorRulesPut proxies PATCH to kapacitor
func (s *Service) KapacitorRulesPut(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	tid := httprouter.GetParamFromContext(ctx, "tid")
	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)
	var req chronograf.AlertRule
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	// TODO: validate this data
	/*
		if err := req.Valid(); err != nil {
			invalidData(w, err)
			return
		}
	*/

	// Check if the rule exists and is scoped correctly
	if _, err = c.Get(ctx, tid); err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, s.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	// Replace alert completely with this new alert.
	req.ID = tid
	task, err := c.Update(ctx, c.Href(tid), req)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}
	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// KapacitorStatus is the current state of a running task
type KapacitorStatus struct {
	Status string `json:"status"`
}

// Valid check if the kapacitor status is enabled or disabled
func (k *KapacitorStatus) Valid() error {
	if k.Status == "enabled" || k.Status == "disabled" {
		return nil
	}
	return fmt.Errorf("Invalid Kapacitor status: %s", k.Status)
}

// KapacitorRulesStatus proxies PATCH to kapacitor to enable/disable tasks
func (s *Service) KapacitorRulesStatus(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	tid := httprouter.GetParamFromContext(ctx, "tid")
	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)

	var req KapacitorStatus
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	// Check if the rule exists and is scoped correctly
	_, err = c.Get(ctx, tid)
	if err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, s.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	var task *kapa.Task
	if req.Status == "enabled" {
		task, err = c.Enable(ctx, c.Href(tid))
	} else {
		task, err = c.Disable(ctx, c.Href(tid))
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// KapacitorRulesGet retrieves all rules
func (s *Service) KapacitorRulesGet(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)
	tasks, err := c.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	res := allAlertsResponse{
		Rules: []*alertResponse{},
	}
	for _, task := range tasks {
		ar := newAlertResponse(task, srv.SrcID, srv.ID)
		res.Rules = append(res.Rules, ar)
	}
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

type allAlertsResponse struct {
	Rules []*alertResponse `json:"rules"`
}

// KapacitorRulesID retrieves specific task
func (s *Service) KapacitorRulesID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}
	tid := httprouter.GetParamFromContext(ctx, "tid")

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)

	// Check if the rule exists within scope
	task, err := c.Get(ctx, tid)
	if err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, s.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// KapacitorRulesDelete proxies DELETE to kapacitor
func (s *Service) KapacitorRulesDelete(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, s.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password)

	tid := httprouter.GetParamFromContext(ctx, "tid")
	// Check if the rule is linked to this server and kapacitor
	if _, err := c.Get(ctx, tid); err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, s.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}
	if err := c.Delete(ctx, c.Href(tid)); err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
